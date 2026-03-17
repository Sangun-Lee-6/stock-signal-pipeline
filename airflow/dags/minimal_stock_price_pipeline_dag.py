import json
import os
from pathlib import Path

import pandas as pd
import pendulum
import psycopg2
from airflow.decorators import dag, task
from pykrx import stock


LOCAL_S3_ROOT = Path("/opt/airflow/s3")


def _int_or_none(value):
    if pd.isna(value):
        return None
    return int(value)


def _float_or_none(value):
    if pd.isna(value):
        return None
    return float(value)


@dag(
    dag_id="minimal_stock_price_pipeline",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["stock-signal", "mvp"],
)
def minimal_stock_price_pipeline():
    @task
    def extract_to_bronze():
        stock_code = os.environ.get("STOCK_SIGNAL_STOCK_CODE", "005930")
        stock_name = stock.get_market_ticker_name(stock_code)

        기준일 = pendulum.now("Asia/Seoul").date().subtract(days=1)
        시작일 = 기준일.subtract(days=7)

        ohlcv_df = stock.get_market_ohlcv_by_date(
            시작일.strftime("%Y%m%d"),
            기준일.strftime("%Y%m%d"),
            stock_code,
        )

        if ohlcv_df.empty:
            raise ValueError(f"{stock_code} 종목의 최근 종가 데이터를 찾지 못했습니다.")

        latest_row = ohlcv_df.reset_index().iloc[-1]
        trade_date = latest_row["날짜"].strftime("%Y-%m-%d")

        raw_payload = {
            "source": "pykrx",
            "stock_code": stock_code,
            "stock_name": stock_name,
            "requested_end_date": 기준일.strftime("%Y-%m-%d"),
            "trade_date": trade_date,
            "collected_at": pendulum.now("Asia/Seoul").to_iso8601_string(),
            "raw": {
                "시가": _int_or_none(latest_row["시가"]),
                "고가": _int_or_none(latest_row["고가"]),
                "저가": _int_or_none(latest_row["저가"]),
                "종가": _int_or_none(latest_row["종가"]),
                "거래량": _int_or_none(latest_row["거래량"]),
                "등락률": _float_or_none(latest_row["등락률"]),
            },
        }

        bronze_dir = (
            LOCAL_S3_ROOT
            / "bronze"
            / "stock_price_raw"
            / f"stock_code={stock_code}"
            / f"trade_date={trade_date}"
        )
        bronze_dir.mkdir(parents=True, exist_ok=True)

        bronze_path = bronze_dir / "data.json"
        with bronze_path.open("w", encoding="utf-8") as file:
            json.dump(raw_payload, file, ensure_ascii=False, indent=2)

        return {
            "stock_code": stock_code,
            "stock_name": stock_name,
            "trade_date": trade_date,
            "bronze_path": str(bronze_path),
        }

    @task
    def transform_to_silver(bronze_result: dict):
        bronze_path = Path(bronze_result["bronze_path"])

        with bronze_path.open("r", encoding="utf-8") as file:
            bronze_payload = json.load(file)

        silver_record = {
            "stock_code": bronze_payload["stock_code"],
            "stock_name": bronze_payload["stock_name"],
            "trade_date": bronze_payload["trade_date"],
            "open_price": bronze_payload["raw"]["시가"],
            "high_price": bronze_payload["raw"]["고가"],
            "low_price": bronze_payload["raw"]["저가"],
            "close_price": bronze_payload["raw"]["종가"],
            "volume": bronze_payload["raw"]["거래량"],
            "price_change_rate": bronze_payload["raw"]["등락률"],
            "source": bronze_payload["source"],
            "bronze_path": str(bronze_path),
            "collected_at": bronze_payload["collected_at"],
        }

        silver_df = pd.DataFrame([silver_record])

        silver_dir = (
            LOCAL_S3_ROOT
            / "silver"
            / "stock_price_daily"
            / f"stock_code={silver_record['stock_code']}"
            / f"trade_date={silver_record['trade_date']}"
        )
        silver_dir.mkdir(parents=True, exist_ok=True)

        silver_path = silver_dir / "data.parquet"
        silver_df.to_parquet(silver_path, index=False)

        return {
            "stock_code": silver_record["stock_code"],
            "trade_date": silver_record["trade_date"],
            "silver_path": str(silver_path),
            "bronze_path": str(bronze_path),
        }

    @task
    def load_to_web_postgres(silver_result: dict):
        silver_df = pd.read_parquet(silver_result["silver_path"])
        row = silver_df.iloc[0]

        connection = psycopg2.connect(
            host=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_HOST", "host.docker.internal"),
            port=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_PORT", "5433"),
            dbname=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_DB", "stock_signal"),
            user=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_USER", "stock_signal"),
            password=os.environ.get(
                "STOCK_SIGNAL_WEB_POSTGRES_PASSWORD",
                "stock_signal_local_pg_password",
            ),
        )

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS stock_price_daily_mart (
                        stock_code VARCHAR(20) NOT NULL,
                        stock_name VARCHAR(100) NOT NULL,
                        trade_date DATE NOT NULL,
                        open_price BIGINT NOT NULL,
                        high_price BIGINT NOT NULL,
                        low_price BIGINT NOT NULL,
                        close_price BIGINT NOT NULL,
                        volume BIGINT NOT NULL,
                        price_change_rate DOUBLE PRECISION,
                        source TEXT NOT NULL,
                        bronze_path TEXT NOT NULL,
                        silver_path TEXT NOT NULL,
                        collected_at TIMESTAMPTZ NOT NULL,
                        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (stock_code, trade_date)
                    )
                    """
                )

                cursor.execute(
                    """
                    INSERT INTO stock_price_daily_mart (
                        stock_code,
                        stock_name,
                        trade_date,
                        open_price,
                        high_price,
                        low_price,
                        close_price,
                        volume,
                        price_change_rate,
                        source,
                        bronze_path,
                        silver_path,
                        collected_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (stock_code, trade_date)
                    DO UPDATE SET
                        stock_name = EXCLUDED.stock_name,
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        price_change_rate = EXCLUDED.price_change_rate,
                        source = EXCLUDED.source,
                        bronze_path = EXCLUDED.bronze_path,
                        silver_path = EXCLUDED.silver_path,
                        collected_at = EXCLUDED.collected_at,
                        loaded_at = NOW()
                    """,
                    (
                        row["stock_code"],
                        row["stock_name"],
                        row["trade_date"],
                        int(row["open_price"]),
                        int(row["high_price"]),
                        int(row["low_price"]),
                        int(row["close_price"]),
                        int(row["volume"]),
                        float(row["price_change_rate"]) if pd.notna(row["price_change_rate"]) else None,
                        row["source"],
                        row["bronze_path"],
                        silver_result["silver_path"],
                        row["collected_at"],
                    ),
                )

        connection.close()

    bronze_result = extract_to_bronze()
    silver_result = transform_to_silver(bronze_result)
    load_to_web_postgres(silver_result)


minimal_stock_price_pipeline()
