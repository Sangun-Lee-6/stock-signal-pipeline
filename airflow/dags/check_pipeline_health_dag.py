from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from google_chat_alert_pipeline import notify_google_chat_on_failure
from pipeline_quality_pipeline import notify_hourly_quality_check_summary


# 최근 품질 체크와 pending silver backlog를 점검하고 실패가 있으면 DAG를 실패시킨다.
def check_pipeline_health(**context):
    import duckdb
    from pathlib import Path
    from mart_ops_pipeline import open_mart_write_lock, summarize_pending_silver_backlog

    logical_date = context["logical_date"].in_timezone("Asia/Seoul")
    quality_summary = notify_hourly_quality_check_summary(logical_date.to_iso8601_string(), 60)
    mart_path = Path("/opt/airflow/s3") / "mart" / "stock_signal.duckdb"
    with open_mart_write_lock():
        with duckdb.connect(str(mart_path), read_only=True) as connection:
            backlog_summary = summarize_pending_silver_backlog(connection, logical_date.to_iso8601_string(), 120)
    stale_backlog_sources = [
        source
        for source, source_summary in backlog_summary["sources"].items()
        if source_summary["pending_count"] > 0 and source_summary["pending_age_minutes"] >= 60
    ]
    if quality_summary["failure_count"] > 0:
        raise ValueError(f"pipeline quality check failures found: failure_count={quality_summary['failure_count']}")
    if stale_backlog_sources:
        raise ValueError(f"pipeline silver backlog found: sources={stale_backlog_sources}")
    return {"quality_summary": quality_summary, "backlog_summary": backlog_summary}


with DAG(
    dag_id="check_pipeline_health",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "on_failure_callback": notify_google_chat_on_failure,
    },
    tags=["stock-signal", "ops", "quality"],
) as check_pipeline_health_dag:
    PythonOperator(
        task_id="check_pipeline_health",
        python_callable=check_pipeline_health,
        execution_timeout=timedelta(minutes=3),
    )
