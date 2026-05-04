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
    @task(execution_timeout=timedelta(minutes=1), pool="duckdb_mart_writer")
    def ensure_loaded_file_table():
        return ensure_mart_loaded_silver_file_table()

    @task(execution_timeout=timedelta(minutes=3), pool="duckdb_mart_writer")
    def load_kis_stock_price_silver():
        import duckdb
        from airflow.operators.python import get_current_context
        from pathlib import Path
        from kis_stock_price_pipeline import insert_stock_price_silver_to_mart
        from mart_ops_pipeline import open_mart_transaction, open_mart_write_lock

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone("Asia/Seoul")
        mart_path = Path("/opt/airflow/s3") / "mart" / "stock_signal.duckdb"
        loaded_count = 0
        with open_mart_write_lock():
            with duckdb.connect(str(mart_path)) as connection:
                pending_results = find_pending_stock_price_silver_results(connection, logical_date.to_iso8601_string(), 3)
                if not pending_results:
                    return {"source": "kis_stock_price", "loaded_count": 0, "mart_path": str(mart_path)}
                loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
                for silver_result in pending_results:
                    with open_mart_transaction(connection) as transaction:
                        insert_stock_price_silver_to_mart(transaction, silver_result, loaded_at)
                        mark_silver_file_mart_loaded(transaction, "kis_stock_price", silver_result["silver_path"], loaded_at, context["dag_run"].dag_id, context["run_id"])
                    loaded_count += 1
        return {"source": "kis_stock_price", "loaded_count": loaded_count, "mart_path": str(mart_path)}

    @task(execution_timeout=timedelta(minutes=3), pool="duckdb_mart_writer")
    def load_mk_rss_silver():
        import duckdb
        from airflow.operators.python import get_current_context
        from pathlib import Path
        from mart_ops_pipeline import open_mart_transaction, open_mart_write_lock
        from mk_rss_pipeline import insert_mk_rss_silver_to_mart

        context = get_current_context()
        logical_date = context["logical_date"].in_timezone("Asia/Seoul")
        if logical_date.minute % 10 != 0:
            return {"source": "mk_rss", "loaded_count": 0, "skipped": True}
        mart_path = Path("/opt/airflow/s3") / "mart" / "stock_signal.duckdb"
        loaded_count = 0
        with open_mart_write_lock():
            with duckdb.connect(str(mart_path)) as connection:
                pending_result = find_pending_mk_rss_silver_result(connection, logical_date.to_iso8601_string(), 20)
                if not pending_result["silver_paths"]:
                    return {"source": "mk_rss", "loaded_count": 0, "mart_path": str(mart_path)}
                loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
                for silver_path in pending_result["silver_paths"]:
                    with open_mart_transaction(connection) as transaction:
                        insert_mk_rss_silver_to_mart(transaction, {**pending_result, "article_count": 1, "silver_paths": [silver_path]}, loaded_at)
                        mark_silver_file_mart_loaded(transaction, "mk_rss", silver_path, loaded_at, context["dag_run"].dag_id, context["run_id"])
                    loaded_count += 1
        return {"source": "mk_rss", "loaded_count": loaded_count, "mart_path": str(mart_path)}

    ensure_task = ensure_loaded_file_table()
    stock_price_task = load_kis_stock_price_silver()
    mk_rss_task = load_mk_rss_silver()

    ensure_task >> stock_price_task >> mk_rss_task


load_silver_to_mart()
