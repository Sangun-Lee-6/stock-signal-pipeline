from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from kis_stock_price_pipeline import write_stock_price_silver_to_mart
from mk_rss_pipeline import write_mk_rss_silver_to_mart


# silver 저장 결과를 순서대로 DuckDB mart에 적재하는 DAG.
# DuckDB 파일 쓰기는 한 DAG run 안에서만 순차 실행한다.
@dag(
    dag_id="load_silver_to_mart",
    schedule=None,
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
    stock_price_task = task(execution_timeout=timedelta(minutes=3))(
        write_stock_price_silver_to_mart
    )(silver_result="{{ dag_run.conf['stock_price_silver_result'] }}")
    mk_rss_task = task(execution_timeout=timedelta(minutes=3))(
        write_mk_rss_silver_to_mart
    )(silver_result="{{ dag_run.conf['mk_rss_silver_result'] }}")

    stock_price_task >> mk_rss_task


load_silver_to_mart()
