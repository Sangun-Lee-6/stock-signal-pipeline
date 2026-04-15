import pendulum
from airflow.decorators import dag, task

from stock_signal.opendart_pipeline import collect_opendart_disclosures_to_bronze


@dag(
    dag_id="collect_opendart_raw",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["stock-signal", "opendart", "raw"],
)
def collect_opendart_raw():
    @task
    def collect_raw():
        return collect_opendart_disclosures_to_bronze()

    collect_raw()


collect_opendart_raw()
