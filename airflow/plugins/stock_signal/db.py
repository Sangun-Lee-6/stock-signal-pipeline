import os # 환경변수를 읽을 때 사용

import psycopg2 # PostgreSQL 연결 드라이버


def create_web_postgres_connection():
    web_postgres_config = {
        "host": os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_HOST", "host.docker.internal"),
        "port": os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_PORT", "5433"),
        "dbname": os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_DB", "stock_signal"),
        "user": os.environ.get("STOCK_SIGNAL_WEB_POSTGRES_USER", "user"),
        "password": os.environ.get(
            "STOCK_SIGNAL_WEB_POSTGRES_PASSWORD",
            "password",
        ),
    }

    return psycopg2.connect(**web_postgres_config)
