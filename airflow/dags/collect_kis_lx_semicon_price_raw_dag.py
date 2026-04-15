import pendulum
from airflow.decorators import dag, task

from stock_signal.kis_stock_price_pipeline import collect_lx_semicon_price_to_bronze


@dag(
    dag_id="collect_kis_lx_semicon_price_raw",
    schedule="0 18 * * 1-5",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False,
    default_args={"owner": "airflow", "retries": 1},
    tags=["stock-signal", "kis", "raw"],
)
def collect_kis_lx_semicon_price_raw():
    @task
    def collect_raw():
        return collect_lx_semicon_price_to_bronze()

    collect_raw()


collect_kis_lx_semicon_price_raw()
