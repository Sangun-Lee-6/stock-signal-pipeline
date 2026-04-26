from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from kis_stock_price_pipeline import (
    collect_stock_price_daily_history_raw,
    write_stock_price_daily_history_bronze_to_silver,
    write_stock_price_daily_history_silver_to_mart,
    write_stock_price_raw_to_bronze,
)


# KIS 일봉 raw 데이터를 하루 1회 수집해서 bronze에 저장하는 DAG.
# 과거 일자 backfill이 가능하도록 logical_date 기준으로 조회 날짜를 고정한다.
@dag(
    dag_id="collect_kis_stock_price_daily_history_raw",
    schedule="0 19 * * 1-5",  # 평일 장 마감 이후 일봉 확정값을 조회한다.
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=True,  # 과거 미실행 구간도 일자별로 backfill 한다.
    max_active_runs=1,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["stock-signal", "kis", "daily-history", "raw", "bronze"],
)
def collect_kis_stock_price_daily_history_raw():
    def collect_daily_history_raw_task(target_start_date, target_end_date):
        return collect_stock_price_daily_history_raw(
            start_date=target_start_date,
            end_date=target_end_date,
        )

    collect_raw_task = task(collect_daily_history_raw_task)(
        target_start_date="{{ ds_nodash }}",
        target_end_date="{{ ds_nodash }}",
    )
    write_raw_to_bronze_task = task(write_stock_price_raw_to_bronze)(
        raw_payload=collect_raw_task,
    )
    write_bronze_to_silver_task = task(
        write_stock_price_daily_history_bronze_to_silver
    )(bronze_result=write_raw_to_bronze_task)
    write_silver_to_mart_task = task(
        write_stock_price_daily_history_silver_to_mart
    )(silver_result=write_bronze_to_silver_task)

    (
        collect_raw_task
        >> write_raw_to_bronze_task
        >> write_bronze_to_silver_task
        >> write_silver_to_mart_task
    )


collect_kis_stock_price_daily_history_raw()
