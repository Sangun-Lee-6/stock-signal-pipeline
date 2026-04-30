from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

from mk_rss_pipeline import (
    collect_mk_rss_raw as collect_mk_rss_raw_payload,
    write_mk_rss_bronze_to_silver,
    write_mk_rss_raw_to_bronze,
)


# MK RSS raw 데이터를 10분 단위로 수집해서 bronze에 저장하는 DAG.
# 오케스트레이션은 DAG가 맡고, 실제 RSS 호출/파일 저장 로직은 plugin 함수가 맡는다.
@dag(
    dag_id="collect_mk_rss_raw",
    schedule="*/10 * * * *",  # 최근 50개 유실을 피하기 위해 10분마다 실행 생성
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),  # 스케줄 계산 기준 시작 시각.
    catchup=False,  # 과거 미실행 구간은 자동으로 backfill 하지 않음
    max_active_runs=1,  # 같은 DAG run이 겹치지 않도록 한 번에 1개만 실행
    default_args={
        "owner": "airflow",  # 기본 owner 메타데이터.
        "retries": 2,  # task 실패 시 재시도 횟수.
        "retry_delay": timedelta(minutes=1),  # 재시도 전 대기 시간.
    },
    tags=["stock-signal", "mk-rss", "raw", "bronze"],
)
def collect_mk_rss_raw():
    # MK RSS를 호출해서 raw payload를 만든다.
    # 이 task는 아직 파일을 쓰지 않고, 다음 task에 넘길 데이터만 반환한다.
    @task(execution_timeout=timedelta(minutes=3))
    def collect_raw():
        return collect_mk_rss_raw_payload()

    # Airflow는 task 반환값을 XCom으로 전달하므로, 여기서는 raw_payload 인자로 받는다.
    @task(execution_timeout=timedelta(minutes=3))
    def write_raw_to_bronze(raw_payload):
        return write_mk_rss_raw_to_bronze(raw_payload)

    # bronze 저장 결과를 읽어 RSS 기사 단위 silver parquet로 변환한다.
    @task(execution_timeout=timedelta(minutes=3))
    def write_bronze_to_silver(bronze_result):
        return write_mk_rss_bronze_to_silver(bronze_result)

    # task 객체를 먼저 만들고, 아래에서 실행 순서를 명시적으로 연결한다.
    collect_raw_task = collect_raw()
    write_raw_to_bronze_task = write_raw_to_bronze(collect_raw_task)
    write_bronze_to_silver_task = write_bronze_to_silver(write_raw_to_bronze_task)

    # 실행 순서:
    # 1. raw 데이터 수집
    # 2. bronze 저장
    # 3. silver 변환
    (
        collect_raw_task
        >> write_raw_to_bronze_task
        >> write_bronze_to_silver_task
    )


collect_mk_rss_raw()
