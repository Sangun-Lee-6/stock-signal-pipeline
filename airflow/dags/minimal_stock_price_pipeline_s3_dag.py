import pendulum
from airflow.decorators import dag, task

from stock_signal.stock_price_pipeline_s3 import run_stock_price_pipeline_to_s3


@dag(
    dag_id="minimal_stock_price_pipeline_s3",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["stock-signal", "mvp", "s3"],
)
def minimal_stock_price_pipeline_s3():
    @task
    def run_pipeline():
        return run_stock_price_pipeline_to_s3()

    run_pipeline()


minimal_stock_price_pipeline_s3()
