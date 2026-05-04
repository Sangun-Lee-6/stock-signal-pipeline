from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from kis_stock_price_pipeline import insert_stock_price_silver_to_mart
from mart_ops_pipeline import (
    ensure_mart_loaded_silver_file_table,
    find_pending_manifest_paths,
    mark_silver_file_mart_loaded,
    open_mart_transaction,
    open_mart_write_lock,
)
from mk_rss_pipeline import MK_SOURCE_FEED, insert_mk_rss_silver_to_mart


# 지정 기간의 manifest를 다시 훑어 미적재 silver file만 mart에 적재한다.
def recover_silver_to_mart(**context):
    import duckdb

    params = context["params"]
    source = str(params["source"])
    start_created_date = str(params["start_created_date"])
    end_created_date = str(params["end_created_date"])
    max_file_count = int(params["max_file_count"])
    mart_path = Path("/opt/airflow/s3") / "mart" / "stock_signal.duckdb"
    ensure_mart_loaded_silver_file_table()
    loaded_count = 0
    with open_mart_write_lock():
        with duckdb.connect(str(mart_path)) as connection:
            pending_paths = find_pending_manifest_paths(connection, source, start_created_date, end_created_date, max_file_count)
            loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
            for pending_path in pending_paths:
                silver_path = pending_path["silver_path"]
                with open_mart_transaction(connection) as transaction:
                    if source == "kis_stock_price":
                        insert_stock_price_silver_to_mart(transaction, pending_path, loaded_at)
                    elif source == "mk_rss":
                        insert_mk_rss_silver_to_mart(transaction, {"collection_id": pending_path["collection_id"], "source_feed": MK_SOURCE_FEED, "article_count": 1, "silver_paths": [silver_path]}, loaded_at)
                    else:
                        raise ValueError(f"unsupported recovery source: {source}")
                    mark_silver_file_mart_loaded(transaction, source, silver_path, loaded_at, context["dag_run"].dag_id, context["run_id"])
                loaded_count += 1
    return {"source": source, "loaded_count": loaded_count, "mart_path": str(mart_path)}


with DAG(
    dag_id="recover_silver_to_mart",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    params={
        "source": "kis_stock_price",
        "start_created_date": pendulum.now("Asia/Seoul").format("YYYY-MM-DD"),
        "end_created_date": pendulum.now("Asia/Seoul").format("YYYY-MM-DD"),
        "max_file_count": 100,
    },
    tags=["stock-signal", "silver", "mart", "recovery"],
) as recover_silver_to_mart_dag:
    PythonOperator(
        task_id="recover_silver_to_mart",
        python_callable=recover_silver_to_mart,
        execution_timeout=timedelta(minutes=10),
        pool="duckdb_mart_writer",
    )
