from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from google_chat_alert_pipeline import notify_google_chat_on_failure
from kis_stock_price_pipeline import (
    collect_stock_price_raw,
    validate_stock_price_bronze_result,
    validate_stock_price_raw_payload,
    validate_stock_price_silver_result,
    write_stock_price_bronze_to_silver,
    write_stock_price_raw_to_bronze,
)
from pipeline_quality_pipeline import (
    ensure_quality_check_passed,
    write_quality_check_result,
)


# KIS 주식 현재가 raw 데이터를 1분 단위로 수집해서 bronze에 저장하는 DAG.
# 오케스트레이션은 DAG가 맡고, 실제 API 호출/파일 저장 로직은 plugin 함수가 맡는다.
@dag(
    dag_id="collect_kis_stock_price_raw",  
    schedule="* 9-15 * * 1-5",  # 평일 09:00~15:59에 1분마다 실행 생성
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),  # 스케줄 계산 기준 시작 시각.
    catchup=False,  # 과거 미실행 구간은 자동으로 backfill 하지 않음
    max_active_runs=1,  # 같은 DAG run이 겹치지 않도록 한 번에 1개만 실행
    default_args={
        "owner": "airflow",  # 기본 owner 메타데이터.
        "retries": 2,  # task 실패 시 재시도 횟수.
        "retry_delay": timedelta(minutes=1),  # 재시도 전 대기 시간.
        "on_failure_callback": notify_google_chat_on_failure, # task 실패 시 Google Chat으로 알림 보내는 콜백 함수 지정
    },
    tags=["stock-signal", "kis", "raw", "bronze"],  
)
def collect_kis_stock_price_raw():
    # 장중 데이터만 수집하기 위해 실행 시각이 시장 시간 안에 있는지 먼저 검사한다.
    # short_circuit task는 False를 반환하면 downstream task를 모두 skip
    @task.short_circuit
    def check_market_window():
        context = get_current_context()
        now_kst = context["logical_date"].in_timezone("Asia/Seoul")
        is_weekday = now_kst.weekday() < 5
        is_after_open = (now_kst.hour, now_kst.minute) >= (9, 0)
        is_before_close = (now_kst.hour, now_kst.minute) <= (15, 30)
        return is_weekday and is_after_open and is_before_close

    # KIS OpenAPI를 호출해서 raw payload를 만든다.
    # 이 task는 아직 파일을 쓰지 않고, 다음 task에 넘길 데이터만 반환한다.
    @task(execution_timeout=timedelta(seconds=50))
    def collect_raw():
        return collect_stock_price_raw()

    # raw payload 구조와 API 응답 성공 여부를 검증한다.
    @task(execution_timeout=timedelta(seconds=50))
    def validate_raw(raw_payload):
        context = get_current_context()
        quality_result = validate_stock_price_raw_payload(raw_payload)
        write_quality_check_result(context, quality_result)
        return ensure_quality_check_passed(quality_result)

    # Airflow는 task 반환값을 XCom으로 전달하므로, 여기서는 raw_payload 인자로 받는다.
    @task(execution_timeout=timedelta(seconds=50))
    def write_raw_to_bronze(raw_payload):
        return write_stock_price_raw_to_bronze(raw_payload)

    # bronze 파일 저장 결과와 JSON 파일 상태를 검증한다.
    @task(execution_timeout=timedelta(seconds=50))
    def validate_bronze(bronze_result):
        context = get_current_context()
        quality_result = validate_stock_price_bronze_result(bronze_result)
        write_quality_check_result(context, quality_result)
        return ensure_quality_check_passed(quality_result)

    # bronze 저장 결과를 읽어 silver parquet로 변환한다.
    @task(execution_timeout=timedelta(seconds=50))
    def write_bronze_to_silver(bronze_result):
        return write_stock_price_bronze_to_silver(bronze_result)

    # silver parquet 저장 결과와 파일 내용을 검증한다.
    @task(execution_timeout=timedelta(seconds=50))
    def validate_silver(silver_result):
        context = get_current_context()
        quality_result = validate_stock_price_silver_result(silver_result)
        write_quality_check_result(context, quality_result)
        return ensure_quality_check_passed(quality_result)

    # task 객체를 먼저 만들고, 아래에서 실행 순서를 명시적으로 연결한다.
    check_market_window_task = check_market_window()
    collect_raw_task = collect_raw()
    validate_raw_task = validate_raw(collect_raw_task)
    write_raw_to_bronze_task = write_raw_to_bronze(collect_raw_task)
    validate_bronze_task = validate_bronze(write_raw_to_bronze_task)
    write_bronze_to_silver_task = write_bronze_to_silver(write_raw_to_bronze_task)
    validate_silver_task = validate_silver(write_bronze_to_silver_task)

    # 실행 순서:
    # 1. 장중 여부 확인
    # 2. raw 데이터 수집
    # 3. raw 품질 체크
    # 4. bronze 저장
    # 5. bronze 품질 체크
    # 6. silver 변환
    # 7. silver 품질 체크
    (
        check_market_window_task
        >> collect_raw_task
        >> validate_raw_task
        >> write_raw_to_bronze_task
        >> validate_bronze_task
        >> write_bronze_to_silver_task
        >> validate_silver_task
    )


collect_kis_stock_price_raw()
