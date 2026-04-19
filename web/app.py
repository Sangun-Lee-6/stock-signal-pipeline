import os
from datetime import date, datetime
from pathlib import Path

import duckdb
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles


app = FastAPI()
STATIC_DIR = Path(__file__).parent / "static"
INDEX_HTML_PATH = STATIC_DIR / "index.html"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def serialize_row(row):
    result = {}
    for key, value in row.items():
        if isinstance(value, (date, datetime)):
            result[key] = value.isoformat()
        else:
            result[key] = value
    return result


def fetch_stock_prices():
    duckdb_path = Path(
        os.environ.get("WEB_DUCKDB_PATH", "/data/mart/stock_signal.duckdb")
    )
    read_only = os.environ.get("WEB_DUCKDB_READ_ONLY", "true").lower() == "true"

    if not duckdb_path.exists():
        return []

    with duckdb.connect(str(duckdb_path), read_only=read_only) as connection:
        objects = {
            (schema, name)
            for schema, name in connection.execute(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                """
            ).fetchall()
        }

        if ("serving", "v_stock_price_timeline") in objects:
            query = """
                SELECT
                    stock_code,
                    stock_name,
                    price_at AS trade_date,
                    open_price,
                    high_price,
                    low_price,
                    current_price AS close_price,
                    volume_accumulated AS volume,
                    change_rate AS price_change_rate,
                    source,
                    NULL AS bronze_path,
                    NULL AS silver_path,
                    collected_at,
                    processed_at AS loaded_at
                FROM serving.v_stock_price_timeline
                ORDER BY trade_date DESC
                LIMIT 10
            """
        elif (
            ("mart", "fact_stock_price") in objects
            and ("mart", "dim_stock") in objects
        ):
            query = """
                SELECT
                    stock.stock_code,
                    stock.stock_name,
                    price.price_at AS trade_date,
                    price.open_price,
                    price.high_price,
                    price.low_price,
                    price.current_price AS close_price,
                    price.volume_accumulated AS volume,
                    price.change_rate AS price_change_rate,
                    price.source,
                    NULL AS bronze_path,
                    NULL AS silver_path,
                    price.collected_at,
                    price.processed_at AS loaded_at
                FROM mart.fact_stock_price AS price
                INNER JOIN mart.dim_stock AS stock
                    ON price.stock_id = stock.stock_id
                ORDER BY trade_date DESC
                LIMIT 10
            """
        else:
            return []

        cursor = connection.execute(query)
        columns = [description[0] for description in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return [serialize_row(row) for row in rows]


@app.get("/")
def read_index():
    return FileResponse(INDEX_HTML_PATH)


@app.get("/api/stock-prices")
def read_stock_prices():
    return JSONResponse(content={"items": fetch_stock_prices()})


@app.get("/health")
def read_health():
    return {"status": "ok"}
