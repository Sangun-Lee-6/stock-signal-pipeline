# 전체 흐름
# 1. pykrx에서 최근 주가 조회
# 2. 원본 형태 그대로 bronze/data.json 저장
# 3. 분석용 스키마로 정리해서 silver/data.parquet 저장
# 4. 최종 서비스용 PostgreSQL 테이블에 upsert 저장
####

import json # 딕셔너리를 JSON 파일로 저장하거나 JSON 파일을 읽을 때 사용, 여기선 bronze의 data.json 저장/읽기에 사용 
import os # 환경변수를 읽을 때 사용
from pathlib import Path # 파일 경로를 문자열보다 안전하게 다루기 위한 표준 라이브러리, `/`로 경로를 이어붙일 수 있어서 가독성이 좋음
import pandas as pd # 여기선 silver 레이어를 parquet로 저장할 때 사용
import pendulum # 날짜⋅시간 처리 라이브러리, 타임존을 다룰 때 사용
import psycopg2 # PostgreSQL 연결 드라이버
from pykrx import stock # 한국 주식 데이터를 가져오는 라이브러리


LOCAL_S3_ROOT = Path("/opt/airflow/s3") # 상수 선언, S3 대신 로컬 파일시스템에서 개발하기 위함, S3로 교체 필요


def extract_stock_price_to_bronze():
    stock_code = os.environ.get("STOCK_SIGNAL_STOCK_CODE", "005930") # 환경변수로 주식 종목 코드를 읽음, 기본값은 삼성전자(005930)
    stock_name = stock.get_market_ticker_name(stock_code) # 종목 코드로 종목명을 조회

    end_date = pendulum.now("Asia/Seoul").date().subtract(days=1) # 종료일 : 어제, 서울 시간 기준으로 오늘 날짜에서 -1
    start_date = end_date.subtract(days=7) # 조회 시작일 : 종료일 -7, 휴장일/주말로 인해 전날 하루만 조회하면 결과가 없을 수 있으므로, 넉넉히 가져와서 가장 최근 거래일 한 건을 고르는 방식

    # pykrx로 OHLCV 데이터 가져오기(기간 : start_date ~ end_date)
    # OHLCV : Open(시가), High(고가), Low(저가), Close(종가), Volume(거래량)
    ohlcv_df = stock.get_market_ohlcv_by_date(
        start_date.strftime("%Y%m%d"), # strftime 메서드로 pykrx가 요구하는 문자열 포맷으로 변경
        end_date.strftime("%Y%m%d"),
        stock_code,
    )
    # 조회 결과가 비어 있으면 예외를 발생
    if ohlcv_df.empty:
        raise ValueError(f"{stock_code} 종목의 최근 종가 데이터를 찾지 못했습니다.")

    latest_row = ohlcv_df.reset_index().iloc[-1] # 가장 최근 거래일 데이터 가져오기
    trade_date = latest_row["날짜"].strftime("%Y-%m-%d") # 가장 최근 거래일을 YYYY-MM-DD 문자열로 바꾸기

    # bronze 데이터
    # 파이썬의 조건 표현식(pd.isna(x) : x가 비어있다면 True)
    # - 결측값 방어: 값이 비어있다면(결측치) int() 변환 시 에러가 나서 런타임 에러 발생 가능
    # - 타입 정규화: JSON 저장할 때 NaN보다 None이 안전함, 파이썬의 None을 JSON으로 바꾸면 null이 됨
    raw_payload = {
        "source": "pykrx",
        "stock_code": stock_code,
        "stock_name": stock_name,
        "requested_end_date": end_date.strftime("%Y-%m-%d"),
        "trade_date": trade_date,
        "collected_at": pendulum.now("Asia/Seoul").to_iso8601_string(),
        "raw": {
            "시가": None if pd.isna(latest_row["시가"]) else int(latest_row["시가"]),
            "고가": None if pd.isna(latest_row["고가"]) else int(latest_row["고가"]),
            "저가": None if pd.isna(latest_row["저가"]) else int(latest_row["저가"]),
            "종가": None if pd.isna(latest_row["종가"]) else int(latest_row["종가"]),
            "거래량": None if pd.isna(latest_row["거래량"]) else int(latest_row["거래량"]),
            "등락률": None if pd.isna(latest_row["등락률"]) else float(latest_row["등락률"]),
        },
    }

    # bronze 데이터 저장 경로 생성
    bronze_dir = (
        LOCAL_S3_ROOT
        / "bronze"
        / "stock_price_raw"
        / f"stock_code={stock_code}"
        / f"trade_date={trade_date}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True) # 상위 폴더까지 생성, 이미 있어도 에러 발생 X

    # bronze 데이터를 JSON 파일로 저장
    bronze_path = bronze_dir / "data.json"
    with bronze_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, ensure_ascii=False, indent=2)

    # 다음 task에서 쓸 최소 정보만 반환
    # XCom payload를 작게 유지
    return {
        "stock_code": stock_code,
        "stock_name": stock_name,
        "trade_date": trade_date,
        "bronze_path": str(bronze_path),
    }


def transform_stock_price_to_silver(bronze_result: dict): # 이전 task의 반환값은 dict
    bronze_path = Path(bronze_result["bronze_path"]) # Path 객체로 변환

    # bronze 파일을 읽어서 dict로 변환
    with bronze_path.open("r", encoding="utf-8") as file:
        bronze_payload = json.load(file)

    # bronze의 원본 중심 구조를 분석용 표준 컬럼 구조로 변경
    # bronze_path, source, collected_at도 같이 넣어서 lineage 추적 가능, 재현 가능성 확보
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

    # dict를 데이터 프레임으로 변경
    silver_df = pd.DataFrame([silver_record])

    # 저장 경로 생성
    silver_dir = (
        LOCAL_S3_ROOT
        / "silver"
        / "stock_price_daily"
        / f"stock_code={silver_record['stock_code']}"
        / f"trade_date={silver_record['trade_date']}"
    )
    silver_dir.mkdir(parents=True, exist_ok=True)

    silver_path = silver_dir / "data.parquet"
    silver_df.to_parquet(silver_path, index=False) # DataFrame을 parquet로 저장, pandas 인덱스를 불필요하게 컬럼으로 저장하지 않음
    # parquet로 저장하는 이유: 컬럼형 저장, 압축 효율 좋음, 읽기 빠름, Spark/DuckDB/Pandas 친화적

    return {
        "stock_code": silver_record["stock_code"],
        "trade_date": silver_record["trade_date"],
        "silver_path": str(silver_path),
        "bronze_path": str(bronze_path),
    }


def load_stock_price_to_web_postgres(silver_result: dict):
    silver_df = pd.read_parquet(silver_result["silver_path"])
    row = silver_df.iloc[0] # 1일 1종목에 대해 1행이므로 df 전체가 아니라 첫 행만 읽음

    # postgresql 연결 생성
    connection = psycopg2.connect(
        host=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_HOST", "host.docker.internal"),
        port=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_PORT", "5433"),
        dbname=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_DB", "stock_signal"),
        user=os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_USER", "user"),
        password=os.environ.get(
            "STOCK_SIGNAL_WEB_POSTGRES_PASSWORD",
            "password",
        ),
    )
    
    with connection: # 트랜잭션 경계
        with connection.cursor() as cursor:

            # 테이블 생성
            # TODO : DDL 분리
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

            # 신규 데이터면 INSERT(stock_code, trade_date 기준)
            # 이미 데이터가 존재하면 update(idempotent load)
            # EXCLUDED : 충돌 시 새로 들어오려던 값을 가리킴
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
