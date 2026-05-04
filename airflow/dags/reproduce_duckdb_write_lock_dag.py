from datetime import timedelta
from pathlib import Path
import time

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def hold_duckdb_write_connection(writer_name, hold_seconds=30):
    import duckdb

    db_path = Path("/opt/airflow/s3/mart/duckdb_lock_repro.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS lock_repro (writer_name VARCHAR, started_at TIMESTAMP)")
        connection.execute("BEGIN TRANSACTION")
        connection.execute("INSERT INTO lock_repro VALUES (?, CAST(? AS TIMESTAMP))", [writer_name, pendulum.now("Asia/Seoul").to_iso8601_string()])
        time.sleep(int(hold_seconds))
        connection.execute("COMMIT")


with DAG(
    dag_id="reproduce_duckdb_write_lock",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
    },
    tags=["stock-signal", "duckdb", "repro", "temporary"],
) as reproduce_duckdb_write_lock_dag:
    start = EmptyOperator(task_id="start")
    writers = [
        PythonOperator(
            task_id=f"writer_{writer_index}",
            python_callable=hold_duckdb_write_connection,
            op_kwargs={"writer_name": f"writer_{writer_index}"},
            execution_timeout=timedelta(minutes=2),
        )
        for writer_index in range(1, 4)
    ]

    start >> writers
