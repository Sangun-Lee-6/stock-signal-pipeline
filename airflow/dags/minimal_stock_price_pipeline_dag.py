import pendulum
from airflow.decorators import dag, task
from stock_signal.stock_price_pipeline import (
    extract_stock_price_to_bronze,
    load_stock_price_to_web_postgres,
    transform_stock_price_to_silver,
)


@dag(
    dag_id="minimal_stock_price_pipeline",
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["stock-signal", "mvp"],
)
def minimal_stock_price_pipeline():
    @task
    def extract_to_bronze():
        return extract_stock_price_to_bronze()

    @task
    def transform_to_silver(bronze_result: dict):
        return transform_stock_price_to_silver(bronze_result)

    @task
    def load_to_web_postgres(silver_result: dict):
        return load_stock_price_to_web_postgres(silver_result)

    bronze_result = extract_to_bronze()
    silver_result = transform_to_silver(bronze_result)
    load_to_web_postgres(silver_result)


minimal_stock_price_pipeline()
