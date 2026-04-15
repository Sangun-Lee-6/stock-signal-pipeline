import io
import json # 딕셔너리를 JSON 문자열로 바꾸거나, S3에서 읽은 JSON 문자열을 dict로 바꿀 때 사용
import os # 환경변수를 읽을 때 사용

import boto3
import pandas as pd # silver 레이어를 parquet로 만들 때 사용
import pendulum # 날짜⋅시간 처리 라이브러리, 타임존을 다룰 때 사용
from pykrx import stock # 한국 주식 데이터를 가져오는 라이브러리

from stock_signal.db import create_web_postgres_connection
from stock_signal.queries import UPSERT_STOCK_PRICE_DAILY_MART_SQL


def run_stock_price_pipeline_to_s3():
    stock_code = os.environ.get("STOCK_SIGNAL_STOCK_CODE", "005930") # 환경변수로 주식 종목 코드를 읽음, 기본값은 삼성전자(005930)
    stock_name = stock.get_market_ticker_name(stock_code) # 종목 코드로 종목명을 조회

    s3_bucket = os.environ.get("S3_BUCKET")
    s3_region = os.environ.get("AWS_REGION", "ap-northeast-2")
    bronze_prefix = os.environ.get("BRONZE_PREFIX", "bronze").strip("/")
    silver_prefix = os.environ.get("SILVER_PREFIX", "silver").strip("/")

    if not s3_bucket:
        raise ValueError("S3_BUCKET 환경변수가 비어 있습니다.")

    session_kwargs = {
        "region_name": s3_region,
    }
    aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session_token = os.environ.get("AWS_SESSION_TOKEN")

    if aws_access_key_id and aws_secret_access_key:
        session_kwargs["aws_access_key_id"] = aws_access_key_id
        session_kwargs["aws_secret_access_key"] = aws_secret_access_key

    if aws_session_token:
        session_kwargs["aws_session_token"] = aws_session_token

    s3_client = boto3.Session(**session_kwargs).client("s3")

    end_date = pendulum.now("Asia/Seoul").date().subtract(days=1) # 종료일 : 어제, 서울 시간 기준으로 오늘 날짜에서 -1
    start_date = end_date.subtract(days=7) # 휴장일/주말을 고려해서 최근 7일 범위로 조회

    ohlcv_df = stock.get_market_ohlcv_by_date(
        start_date.strftime("%Y%m%d"),
        end_date.strftime("%Y%m%d"),
        stock_code,
    )
    if ohlcv_df.empty:
        raise ValueError(f"{stock_code} 종목의 최근 종가 데이터를 찾지 못했습니다.")

    latest_row = ohlcv_df.reset_index().iloc[-1]
    trade_date = latest_row["날짜"].strftime("%Y-%m-%d")
    collected_at = pendulum.now("Asia/Seoul").to_iso8601_string()

    bronze_payload = {
        "source": "pykrx",
        "stock_code": stock_code,
        "stock_name": stock_name,
        "requested_end_date": end_date.strftime("%Y-%m-%d"),
        "trade_date": trade_date,
        "collected_at": collected_at,
        "raw": {
            "시가": None if pd.isna(latest_row["시가"]) else int(latest_row["시가"]),
            "고가": None if pd.isna(latest_row["고가"]) else int(latest_row["고가"]),
            "저가": None if pd.isna(latest_row["저가"]) else int(latest_row["저가"]),
            "종가": None if pd.isna(latest_row["종가"]) else int(latest_row["종가"]),
            "거래량": None if pd.isna(latest_row["거래량"]) else int(latest_row["거래량"]),
            "등락률": None if pd.isna(latest_row["등락률"]) else float(latest_row["등락률"]),
        },
    }

    bronze_key = (
        f"{bronze_prefix}/stock_price_raw/"
        f"stock_code={stock_code}/trade_date={trade_date}/data.json"
    )
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=bronze_key,
        Body=json.dumps(bronze_payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    bronze_s3_path = f"s3://{s3_bucket}/{bronze_key}"

    bronze_object = s3_client.get_object(Bucket=s3_bucket, Key=bronze_key)
    bronze_payload_from_s3 = json.loads(bronze_object["Body"].read().decode("utf-8"))

    silver_record = {
        "stock_code": bronze_payload_from_s3["stock_code"],
        "stock_name": bronze_payload_from_s3["stock_name"],
        "trade_date": bronze_payload_from_s3["trade_date"],
        "open_price": bronze_payload_from_s3["raw"]["시가"],
        "high_price": bronze_payload_from_s3["raw"]["고가"],
        "low_price": bronze_payload_from_s3["raw"]["저가"],
        "close_price": bronze_payload_from_s3["raw"]["종가"],
        "volume": bronze_payload_from_s3["raw"]["거래량"],
        "price_change_rate": bronze_payload_from_s3["raw"]["등락률"],
        "source": bronze_payload_from_s3["source"],
        "bronze_path": bronze_s3_path,
        "collected_at": bronze_payload_from_s3["collected_at"],
    }

    silver_df = pd.DataFrame([silver_record])
    silver_buffer = io.BytesIO()
    silver_df.to_parquet(silver_buffer, index=False) # DataFrame을 parquet로 저장, pandas 인덱스를 불필요하게 컬럼으로 저장하지 않음
    silver_buffer.seek(0)

    silver_key = (
        f"{silver_prefix}/stock_price_daily/"
        f"stock_code={silver_record['stock_code']}/trade_date={silver_record['trade_date']}/data.parquet"
    )
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=silver_key,
        Body=silver_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    silver_s3_path = f"s3://{s3_bucket}/{silver_key}"

    row = silver_df.iloc[0] # 1일 1종목에 대해 1행이므로 df 전체가 아니라 첫 행만 읽음
    connection = create_web_postgres_connection()

    with connection: # 트랜잭션 경계
        with connection.cursor() as cursor:
            cursor.execute(
                UPSERT_STOCK_PRICE_DAILY_MART_SQL,
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
                    bronze_s3_path,
                    silver_s3_path,
                    row["collected_at"],
                ),
            )

    connection.close()

    return {
        "stock_code": stock_code,
        "trade_date": trade_date,
        "bronze_path": bronze_s3_path,
        "silver_path": silver_s3_path,
    }
