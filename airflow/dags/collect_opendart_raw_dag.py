from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

from opendart_pipeline import (
    collect_opendart_page_manifest,
    collect_opendart_raw_page,
    write_opendart_bronze_to_silver,
    write_opendart_raw_to_bronze,
    write_opendart_silver_to_mart,
)


# OpenDART 공시 목록 raw 데이터를 하루 1회 수집해서 bronze에 저장하는 DAG.
# 오케스트레이션은 DAG가 맡고, 실제 API 호출/파일 저장 로직은 plugin 함수가 맡는다.
@dag(
    dag_id="collect_opendart_raw",
    schedule="0 19 * * *",  # 공시가 대부분 마감된 저녁 시간대에 하루 1회 실행 생성
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),  # 스케줄 계산 기준 시작 시각.
    catchup=False,  # 과거 미실행 구간은 자동으로 backfill 하지 않음
    max_active_runs=1,  # 같은 DAG run이 겹치지 않도록 한 번에 1개만 실행
    default_args={
        "owner": "airflow",  # 기본 owner 메타데이터.
        "retries": 2,  # task 실패 시 재시도 횟수.
        "retry_delay": timedelta(minutes=1),  # 재시도 전 대기 시간.
    },
    tags=["stock-signal", "opendart", "raw", "bronze"],
)
def collect_opendart_raw():
    # 실행 날짜를 OpenDART 조회 파라미터로 바꿔 page manifest를 먼저 만든다.
    # 이 task는 전체 페이지 수와 공통 메타데이터만 반환한다.
    @task
    def collect_page_manifest():
        logical_date = get_current_context()["logical_date"].in_timezone("Asia/Seoul")
        target_date = logical_date.format("YYYYMMDD")
        return collect_opendart_page_manifest({"bgn_de": target_date, "end_de": target_date})

    # manifest를 바탕으로 page별 수집 요청 목록을 만든다.
    @task
    def build_page_requests(page_manifest):
        request_params = page_manifest["request"]["params"]
        total_page = int(page_manifest["response"]["total_page"])
        return [
            {
                "bgn_de": request_params["bgn_de"],
                "end_de": request_params["end_de"],
                "last_reprt_at": request_params["last_reprt_at"],
                "sort": request_params["sort"],
                "sort_mth": request_params["sort_mth"],
                "page_count": request_params["page_count"],
                "collection_id": page_manifest["collection_id"],
                "collected_at": page_manifest["collected_at"],
                "page_no": str(page_no),
            }
            for page_no in range(1, total_page + 1)
        ]

    # page별 OpenDART raw payload를 수집한다.
    @task
    def collect_raw_page(page_request):
        return collect_opendart_raw_page(page_request)

    # Airflow는 task 반환값을 XCom으로 전달하므로, 여기서는 raw_payload 인자로 받는다.
    @task
    def write_raw_to_bronze(raw_payload):
        return write_opendart_raw_to_bronze(raw_payload)

    # page별 bronze 저장 결과를 읽어 공시 1건 단위 silver parquet로 변환한다.
    @task
    def write_bronze_to_silver(bronze_result):
        return write_opendart_bronze_to_silver(bronze_result)

    # silver parquet 공시들을 DuckDB mart 이벤트 테이블과 serving view에 적재한다.
    @task
    def write_silver_to_mart(silver_result):
        return write_opendart_silver_to_mart(silver_result)

    # 실행 순서:
    # 1. page manifest 수집
    # 2. page별 수집 요청 생성
    # 3. page별 raw 데이터 수집
    # 4. page별 bronze 저장
    # 5. page별 silver 변환
    # 6. page별 DuckDB mart 적재
    page_manifest_task = collect_page_manifest()
    page_requests_task = build_page_requests(page_manifest_task)
    raw_payloads_task = collect_raw_page.expand(page_request=page_requests_task)
    bronze_results_task = write_raw_to_bronze.expand(raw_payload=raw_payloads_task)
    silver_results_task = write_bronze_to_silver.expand(bronze_result=bronze_results_task)
    write_silver_to_mart.expand(silver_result=silver_results_task)


collect_opendart_raw()
