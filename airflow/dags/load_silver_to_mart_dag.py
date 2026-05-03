from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from kis_stock_price_pipeline import find_pending_stock_price_silver_results
from mart_ops_pipeline import ensure_mart_loaded_silver_file_table, mark_silver_file_mart_loaded
from mk_rss_pipeline import find_pending_mk_rss_silver_result


# silver 저장 결과를 순서대로 DuckDB mart에 적재하는 DAG.
# DuckDB 파일 쓰기는 한 DAG run 안에서만 순차 실행한다.
@dag(
    dag_id="load_silver_to_mart",
    schedule="* * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["stock-signal", "silver", "mart"],
)
def load_silver_to_mart():
    @task(execution_timeout=timedelta(minutes=1))
    def ensure_loaded_file_table():
        return ensure_mart_loaded_silver_file_table()

    @task(execution_timeout=timedelta(minutes=3))
    def load_kis_stock_price_silver():
        import duckdb
        from airflow.operators.python import get_current_context
        from pathlib import Path

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone("Asia/Seoul")
        pending_results = find_pending_stock_price_silver_results(logical_date.to_iso8601_string(), 3)
        mart_path = Path("/opt/airflow/s3") / "mart" / "stock_signal.duckdb"
        if not pending_results:
            return {"source": "kis_stock_price", "loaded_count": 0, "mart_path": str(mart_path)}
        loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
        with duckdb.connect(str(mart_path)) as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute("CREATE SCHEMA IF NOT EXISTS mart")
                connection.execute("CREATE SCHEMA IF NOT EXISTS serving")
                connection.execute("CREATE TABLE IF NOT EXISTS mart.dim_stock (stock_id BIGINT, stock_code VARCHAR, stock_name VARCHAR, market_division_code VARCHAR, market_name VARCHAR, industry_name VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
                connection.execute("CREATE TABLE IF NOT EXISTS mart.fact_stock_price (stock_id BIGINT, price_at TIMESTAMP, price_date DATE, current_price DECIMAL(18,2), open_price DECIMAL(18,2), high_price DECIMAL(18,2), low_price DECIMAL(18,2), change_rate DECIMAL(9,4), volume_accumulated BIGINT, trade_amount_accumulated DECIMAL(18,2), source VARCHAR, collection_id VARCHAR, collected_at TIMESTAMP, processed_at TIMESTAMP)")
                for silver_result in pending_results:
                    silver_path = silver_result["silver_path"]
                    connection.execute("INSERT INTO mart.dim_stock SELECT COALESCE((SELECT MAX(stock_id) FROM mart.dim_stock), 0) + ROW_NUMBER() OVER (ORDER BY src.stock_code), src.stock_code, src.stock_name, src.market_division_code, src.market_name, src.industry_name, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM (SELECT DISTINCT stock_code, stock_name, market_division_code, market_name, industry_name FROM read_parquet(?)) AS src WHERE NOT EXISTS (SELECT 1 FROM mart.dim_stock AS dim WHERE dim.stock_code = src.stock_code)", [loaded_at, loaded_at, silver_path])
                    connection.execute("INSERT INTO mart.fact_stock_price SELECT dim.stock_id, CAST(src.price_at AS TIMESTAMP), CAST(src.price_date AS DATE), CAST(src.current_price AS DECIMAL(18,2)), CAST(src.open_price AS DECIMAL(18,2)), CAST(src.high_price AS DECIMAL(18,2)), CAST(src.low_price AS DECIMAL(18,2)), CAST(src.change_rate AS DECIMAL(9,4)), CAST(src.volume_accumulated AS BIGINT), CAST(src.trade_amount_accumulated AS DECIMAL(18,2)), src.source, src.collection_id, CAST(src.collected_at AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src INNER JOIN mart.dim_stock AS dim ON src.stock_code = dim.stock_code WHERE NOT EXISTS (SELECT 1 FROM mart.fact_stock_price AS fact WHERE fact.stock_id = dim.stock_id AND fact.price_at = CAST(src.price_at AS TIMESTAMP))", [loaded_at, silver_path])
                    mark_silver_file_mart_loaded(connection, "kis_stock_price", silver_path, loaded_at, context["dag_run"].dag_id, context["run_id"])
                connection.execute("CREATE OR REPLACE VIEW serving.v_stock_price_timeline AS SELECT stock.stock_code, stock.stock_name, price.price_at, price.price_date, price.current_price, price.open_price, price.high_price, price.low_price, price.change_rate, price.volume_accumulated, price.trade_amount_accumulated, price.source, price.collection_id, price.collected_at, price.processed_at FROM mart.fact_stock_price AS price INNER JOIN mart.dim_stock AS stock ON price.stock_id = stock.stock_id")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return {"source": "kis_stock_price", "loaded_count": len(pending_results), "mart_path": str(mart_path)}

    @task(execution_timeout=timedelta(minutes=3))
    def load_mk_rss_silver():
        import duckdb
        from airflow.operators.python import get_current_context
        from pathlib import Path

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone("Asia/Seoul")
        if logical_date.minute % 10 != 0:
            return {"source": "mk_rss", "loaded_count": 0, "skipped": True}
        pending_result = find_pending_mk_rss_silver_result(logical_date.to_iso8601_string(), 20)
        mart_path = Path("/opt/airflow/s3") / "mart" / "stock_signal.duckdb"
        if not pending_result["silver_paths"]:
            return {"source": "mk_rss", "loaded_count": 0, "mart_path": str(mart_path)}
        loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
        with duckdb.connect(str(mart_path)) as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute("CREATE SCHEMA IF NOT EXISTS mart")
                connection.execute("CREATE SCHEMA IF NOT EXISTS serving")
                connection.execute("CREATE TABLE IF NOT EXISTS mart.dim_stock (stock_id BIGINT, stock_code VARCHAR, stock_name VARCHAR, market_division_code VARCHAR, market_name VARCHAR, industry_name VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
                connection.execute("CREATE TABLE IF NOT EXISTS mart.dim_event_source (event_source_id BIGINT, event_source_code VARCHAR, event_source_name VARCHAR, event_source_type VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
                connection.execute("CREATE TABLE IF NOT EXISTS mart.fact_market_event (event_id VARCHAR, event_source_id BIGINT, stock_id BIGINT, event_scope VARCHAR, event_at TIMESTAMP, event_date DATE, event_title VARCHAR, event_summary VARCHAR, event_url VARCHAR, source_record_id VARCHAR, is_main_event BOOLEAN, source VARCHAR, collection_id VARCHAR, collected_at TIMESTAMP, processed_at TIMESTAMP)")
                connection.execute("CREATE TABLE IF NOT EXISTS mart.fact_market_event_classification (event_id VARCHAR, standardized_title VARCHAR, impact_scope VARCHAR, scope_evidence VARCHAR, driver_category VARCHAR, driver_evidence VARCHAR, impact_direction VARCHAR, direction_evidence VARCHAR, matched_entities VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
                connection.execute("INSERT INTO mart.dim_event_source SELECT COALESCE((SELECT MAX(event_source_id) FROM mart.dim_event_source), 0) + 1, 'mk_rss_news', 'MK RSS News', 'news', CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) WHERE NOT EXISTS (SELECT 1 FROM mart.dim_event_source WHERE event_source_code = 'mk_rss_news')", [loaded_at, loaded_at])
                for silver_path in pending_result["silver_paths"]:
                    connection.execute("INSERT INTO mart.fact_market_event SELECT 'mk_rss:' || src.article_id, source_dim.event_source_id, NULL, 'market', CAST(src.published_at AS TIMESTAMP), CAST(src.published_date AS DATE), COALESCE(src.standardized_title, src.title), src.description, src.article_url, src.article_id, TRUE, src.source, src.collection_id, CAST(src.collected_at AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src CROSS JOIN (SELECT event_source_id FROM mart.dim_event_source WHERE event_source_code = 'mk_rss_news') AS source_dim WHERE NOT EXISTS (SELECT 1 FROM mart.fact_market_event AS fact WHERE fact.event_id = 'mk_rss:' || src.article_id AND fact.event_scope = 'market')", [loaded_at, silver_path])
                    connection.execute("INSERT INTO mart.fact_market_event_classification SELECT 'mk_rss:' || src.article_id, src.standardized_title, src.impact_scope, src.scope_evidence, src.driver_category, src.driver_evidence, src.impact_direction, src.direction_evidence, src.matched_entities, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src WHERE NOT EXISTS (SELECT 1 FROM mart.fact_market_event_classification AS classification WHERE classification.event_id = 'mk_rss:' || src.article_id)", [loaded_at, loaded_at, silver_path])
                    mark_silver_file_mart_loaded(connection, "mk_rss", silver_path, loaded_at, context["dag_run"].dag_id, context["run_id"])
                connection.execute("CREATE OR REPLACE VIEW serving.v_stock_event_timeline AS SELECT stock.stock_code, stock.stock_name, source_dim.event_source_code, source_dim.event_source_name, source_dim.event_source_type, event.event_id, event.event_scope, event.event_at, event.event_date, event.event_title, event.event_summary, event.event_url, event.source_record_id, event.is_main_event, event.source, event.collection_id, event.collected_at, event.processed_at, classification.standardized_title, classification.impact_scope, classification.scope_evidence, classification.driver_category, classification.driver_evidence, classification.impact_direction, classification.direction_evidence, classification.matched_entities FROM mart.fact_market_event AS event INNER JOIN mart.dim_event_source AS source_dim ON event.event_source_id = source_dim.event_source_id LEFT JOIN mart.dim_stock AS stock ON event.stock_id = stock.stock_id LEFT JOIN mart.fact_market_event_classification AS classification ON event.event_id = classification.event_id")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return {"source": "mk_rss", "loaded_count": pending_result["article_count"], "mart_path": str(mart_path)}

    ensure_task = ensure_loaded_file_table()
    stock_price_task = load_kis_stock_price_silver()
    mk_rss_task = load_mk_rss_silver()

    ensure_task >> stock_price_task >> mk_rss_task


load_silver_to_mart()
