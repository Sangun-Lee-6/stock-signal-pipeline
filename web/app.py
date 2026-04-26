import os
from decimal import Decimal
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
        elif isinstance(value, Decimal):
            result[key] = float(value)
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
def read_stock_prices(stock_code: str | None = None, range: str = "1m"):
    duckdb_path = Path(
        os.environ.get("WEB_DUCKDB_PATH", "/data/mart/stock_signal.duckdb")
    )
    read_only = os.environ.get("WEB_DUCKDB_READ_ONLY", "true").lower() == "true"
    selected_range = range if range in {"1d", "5d", "1m", "6m"} else "1m"

    if not duckdb_path.exists():
        return JSONResponse(content={"stocks": [], "items": []})

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

        if ("serving", "v_stock_price_daily") in objects:
            base_query = """
                SELECT
                    stock_code,
                    stock_name,
                    price_date AS trade_date,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume_accumulated AS volume,
                    change_rate AS price_change_rate,
                    source,
                    collected_at,
                    processed_at AS loaded_at
                FROM serving.v_stock_price_daily
            """
            uses_daily_range = True
        elif (
            ("mart", "fact_stock_price_daily") in objects
            and ("mart", "dim_stock") in objects
        ):
            base_query = """
                SELECT
                    stock.stock_code,
                    stock.stock_name,
                    price.price_date AS trade_date,
                    price.open_price,
                    price.high_price,
                    price.low_price,
                    price.close_price,
                    price.volume_accumulated AS volume,
                    price.change_rate AS price_change_rate,
                    price.source,
                    price.collected_at,
                    price.processed_at AS loaded_at
                FROM mart.fact_stock_price_daily AS price
                INNER JOIN mart.dim_stock AS stock
                    ON price.stock_id = stock.stock_id
            """
            uses_daily_range = True
        elif ("serving", "v_stock_price_timeline") in objects:
            base_query = """
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
                    collected_at,
                    processed_at AS loaded_at
                FROM serving.v_stock_price_timeline
            """
            uses_daily_range = False
        elif (
            ("mart", "fact_stock_price") in objects
            and ("mart", "dim_stock") in objects
        ):
            base_query = """
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
                    price.collected_at,
                    price.processed_at AS loaded_at
                FROM mart.fact_stock_price AS price
                INNER JOIN mart.dim_stock AS stock
                    ON price.stock_id = stock.stock_id
            """
            uses_daily_range = False
        else:
            return JSONResponse(content={"stocks": [], "items": []})

        stock_cursor = connection.execute(
            f"""
            WITH price_rows AS (
                {base_query}
            )
            SELECT
                stock_code,
                stock_name,
                MAX(trade_date) AS last_trade_date,
                arg_max(close_price, trade_date) AS last_close_price
            FROM price_rows
            GROUP BY stock_code, stock_name
            ORDER BY stock_name
            """
        )
        stock_columns = [description[0] for description in stock_cursor.description]
        stock_rows = [dict(zip(stock_columns, row)) for row in stock_cursor.fetchall()]

        if uses_daily_range:
            range_filter = {
                "1d": "price_rank <= 1",
                "5d": "price_rank <= 5",
                "1m": "trade_date >= max_trade_date - INTERVAL 1 MONTH",
                "6m": "trade_date >= max_trade_date - INTERVAL 6 MONTH",
            }[selected_range]
            item_query = f"""
                WITH price_rows AS (
                    {base_query}
                ),
                filtered_price_rows AS (
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
                        collected_at,
                        loaded_at,
                        MAX(trade_date) OVER (
                            PARTITION BY stock_code
                        ) AS max_trade_date,
                        ROW_NUMBER() OVER (
                            PARTITION BY stock_code
                            ORDER BY trade_date DESC
                        ) AS price_rank
                    FROM price_rows
                    WHERE (? IS NULL OR stock_code = ?)
                )
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
                    NULL AS bronze_path,
                    NULL AS silver_path,
                    collected_at,
                    loaded_at
                FROM filtered_price_rows
                WHERE {range_filter}
                ORDER BY trade_date ASC, stock_code ASC
            """
            item_cursor = connection.execute(item_query, [stock_code, stock_code])
        else:
            item_query = f"""
                WITH price_rows AS (
                    {base_query}
                ),
                latest_price_rows AS (
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
                        collected_at,
                        loaded_at
                    FROM price_rows
                    WHERE (? IS NULL OR stock_code = ?)
                    ORDER BY trade_date DESC, stock_code DESC
                    LIMIT 240
                )
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
                    NULL AS bronze_path,
                    NULL AS silver_path,
                    collected_at,
                    loaded_at
                FROM latest_price_rows
                ORDER BY trade_date ASC, stock_code ASC
            """
            item_cursor = connection.execute(item_query, [stock_code, stock_code])

        item_columns = [description[0] for description in item_cursor.description]
        item_rows = [dict(zip(item_columns, row)) for row in item_cursor.fetchall()]
        return JSONResponse(
            content={
                "stocks": [serialize_row(row) for row in stock_rows],
                "items": [serialize_row(row) for row in item_rows],
                "selected_stock_code": stock_code,
                "selected_range": selected_range,
            }
        )


def read_stock_events(stock_code: str | None = None):
    duckdb_path = Path(
        os.environ.get("WEB_DUCKDB_PATH", "/data/mart/stock_signal.duckdb")
    )
    read_only = os.environ.get("WEB_DUCKDB_READ_ONLY", "true").lower() == "true"

    if not duckdb_path.exists():
        return {"items": []}

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

        if ("serving", "v_stock_event_timeline") in objects:
            query = """
                SELECT
                    stock_code,
                    stock_name,
                    event_source_code,
                    event_source_name,
                    event_scope,
                    event_id,
                    event_at,
                    event_date,
                    event_title,
                    event_summary,
                    event_url,
                    standardized_title,
                    impact_scope,
                    scope_evidence,
                    driver_category,
                    driver_evidence,
                    impact_direction,
                    direction_evidence,
                    matched_entities,
                    source
                FROM serving.v_stock_event_timeline
                WHERE (? IS NULL OR stock_code = ? OR event_scope = 'market')
                ORDER BY
                    CASE impact_scope
                        WHEN '시장전체' THEN 1
                        WHEN '섹터' THEN 2
                        WHEN '기업' THEN 3
                        ELSE 4
                    END,
                    event_at DESC NULLS LAST,
                    event_id DESC
                LIMIT 20
            """
        else:
            return {"items": []}

        cursor = connection.execute(query, [stock_code, stock_code])
        columns = [description[0] for description in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return {"items": [serialize_row(row) for row in rows]}


app.add_api_route("/api/stock-events", read_stock_events, methods=["GET"])


@app.get("/health")
def read_health():
    return {"status": "ok"}
