import os
from datetime import date, datetime
from pathlib import Path

import psycopg2
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from psycopg2.extras import RealDictCursor


app = FastAPI()
STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML_PATH = STATIC_DIR / "index.html"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("WEB_POSTGRES_HOST", "web-postgres"),
        port=os.environ.get("WEB_POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "stock_signal"),
        user=os.environ.get("POSTGRES_USER", "user"),
        password=os.environ.get("POSTGRES_PASSWORD", "password"),
    )


def serialize_row(row):
    result = {}
    for key, value in row.items():
        if isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def fetch_stock_prices():
    with get_connection() as connection:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT
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
                    collected_at,
                    loaded_at
                FROM stock_price_daily_mart
                ORDER BY trade_date DESC
                LIMIT 10
                """
            )
            return [serialize_row(row) for row in cursor.fetchall()]


@app.get("/")
def read_index():
    return FileResponse(INDEX_HTML_PATH)


@app.get("/api/stock-prices")
def read_stock_prices():
    return JSONResponse(content={"items": fetch_stock_prices()})


@app.get("/health")
def read_health():
    return {"status": "ok"}
