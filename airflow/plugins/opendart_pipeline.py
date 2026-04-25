import json
import os
import time
import uuid
from pathlib import Path
from urllib import error, parse, request

import pendulum


LOCAL_S3_ROOT = Path("/opt/airflow/s3")
DEFAULT_OPENDART_BASE_URL = "https://opendart.fss.or.kr"
OPENDART_DISCLOSURE_LIST_ENDPOINT = "/api/list.json"
OPENDART_SOURCE = "opendart"
OPENDART_USER_AGENT = "stock-signal-pipeline/opendart-raw-ingestion"
OPENDART_REQUEST_MAX_ATTEMPTS = 3
OPENDART_REQUEST_INTERVAL_SECONDS = 0.2
DEFAULT_LAST_REPRT_AT = "Y"
DEFAULT_SORT = "date"
DEFAULT_SORT_MTH = "desc"
DEFAULT_PAGE_COUNT = "100"
VALID_OPENDART_STATUS_CODES = {"000", "013"}


# 지정한 날짜 범위의 OpenDART 공시 목록 page manifest를 수집해 페이지 수를 반환한다.
def collect_opendart_page_manifest(query_params):
    api_key = _read_required_env(
        "OPENDART_API_KEY 환경변수가 비어 있습니다.",
        "OPENDART_API_KEY",
        "OPEN_DART_API_KEY",
        "DART_API_KEY",
    )
    base_url = _read_base_url()
    collected_at = pendulum.now("Asia/Seoul")
    collection_id = _build_collection_id(collected_at)
    request_params = _build_request_params(query_params, api_key)
    first_page_response = _request_page_response(base_url, request_params, 1)
    page_manifest = _build_page_manifest(
        base_url,
        collected_at,
        collection_id,
        request_params,
        first_page_response,
    )
    manifest_dir = (
        LOCAL_S3_ROOT
        / "bronze"
        / OPENDART_SOURCE
        / f"bgn_de={request_params['bgn_de']}"
        / f"end_de={request_params['end_de']}"
        / f"collection_id={collection_id}"
    )
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / "manifest.json"
    with manifest_path.open("w", encoding="utf-8") as file:
        json.dump(page_manifest, file, ensure_ascii=False, indent=2)
    page_manifest["manifest_path"] = str(manifest_path)
    return page_manifest


# 지정한 페이지의 OpenDART 공시 목록 raw 응답 1건을 수집하고 bronze 저장용 payload를 반환한다.
def collect_opendart_raw_page(page_request):
    api_key = _read_required_env(
        "OPENDART_API_KEY 환경변수가 비어 있습니다.",
        "OPENDART_API_KEY",
        "OPEN_DART_API_KEY",
        "DART_API_KEY",
    )
    base_url = _read_base_url()
    request_params = _build_request_params(page_request, api_key)
    page_params = _build_page_params(request_params, page_request["page_no"])
    page_response = _request_page_response(base_url, request_params, page_request["page_no"])
    return _build_raw_payload(base_url, page_request, page_params, page_response)


# 수집한 OpenDART raw payload를 bronze 경로에 저장하고 저장 결과만 반환한다.
def write_opendart_raw_to_bronze(raw_payload):
    bronze_path = _write_bronze_payload(raw_payload)
    return _build_write_result(raw_payload, bronze_path)


# bronze 저장 결과 dict를 읽어 OpenDART 공시 1건 단위 silver parquet들로 저장하고 저장 결과를 반환한다.
def write_opendart_bronze_to_silver(bronze_result):
    import pandas as pd

    bronze_path = Path(bronze_result["bronze_path"])
    raw_payload = json.loads(bronze_path.read_text(encoding="utf-8"))
    response_body = raw_payload["response"]["body"]
    collected_at = raw_payload["collected_at"]
    processed_at = pendulum.now("Asia/Seoul").to_iso8601_string()
    silver_paths = []
    for disclosure in response_body.get("list", []):
        disclosure_id = str(disclosure.get("rcept_no") or "").strip()
        if not disclosure_id:
            continue
        event_date = str(disclosure.get("rcept_dt") or "").strip()
        event_at = (
            pendulum.from_format(event_date, "YYYYMMDD", tz="Asia/Seoul")
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .to_iso8601_string()
            if event_date
            else None
        )
        silver_path = (
            LOCAL_S3_ROOT
            / "silver"
            / "silver_disclosure_event"
            / f"event_date={pendulum.from_format(event_date, 'YYYYMMDD', tz='Asia/Seoul').format('YYYY-MM-DD')}"
            / f"disclosure_id={disclosure_id}"
            / "data.parquet"
        )
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "source": raw_payload["source"],
                    "collection_id": raw_payload["collection_id"],
                    "disclosure_id": disclosure_id,
                    "corp_code": disclosure.get("corp_code"),
                    "corp_name": disclosure.get("corp_name"),
                    "stock_code": disclosure.get("stock_code") or None,
                    "corp_cls": disclosure.get("corp_cls") or None,
                    "report_name": disclosure.get("report_nm"),
                    "filer_name": disclosure.get("flr_nm") or None,
                    "remark": disclosure.get("rm") or None,
                    "event_date": pendulum.from_format(event_date, "YYYYMMDD", tz="Asia/Seoul").format("YYYY-MM-DD"),
                    "event_at": event_at,
                    "collected_at": collected_at,
                    "processed_at": processed_at,
                }
            ]
        ).to_parquet(silver_path, index=False)
        silver_paths.append(str(silver_path))
    return {
        "collection_id": raw_payload["collection_id"],
        "bgn_de": raw_payload["request"]["params"]["bgn_de"],
        "end_de": raw_payload["request"]["params"]["end_de"],
        "page_no": raw_payload["request"]["params"]["page_no"],
        "disclosure_count": len(silver_paths),
        "silver_paths": silver_paths,
    }


# silver 저장 결과 dict를 읽어 OpenDART 공시들을 DuckDB mart 이벤트 테이블과 serving view에 적재한다.
def write_opendart_silver_to_mart(silver_result):
    import duckdb

    mart_path = LOCAL_S3_ROOT / "mart" / "stock_signal.duckdb"
    loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
    mart_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(mart_path)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS mart")
        connection.execute("CREATE SCHEMA IF NOT EXISTS serving")
        connection.execute("CREATE TABLE IF NOT EXISTS mart.dim_stock (stock_id BIGINT, stock_code VARCHAR, stock_name VARCHAR, market_division_code VARCHAR, market_name VARCHAR, industry_name VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
        connection.execute("CREATE TABLE IF NOT EXISTS mart.dim_event_source (event_source_id BIGINT, event_source_code VARCHAR, event_source_name VARCHAR, event_source_type VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
        connection.execute("CREATE TABLE IF NOT EXISTS mart.fact_market_event (event_id VARCHAR, event_source_id BIGINT, stock_id BIGINT, event_scope VARCHAR, event_at TIMESTAMP, event_date DATE, event_title VARCHAR, event_summary VARCHAR, event_url VARCHAR, source_record_id VARCHAR, is_main_event BOOLEAN, source VARCHAR, collection_id VARCHAR, collected_at TIMESTAMP, processed_at TIMESTAMP)")
        connection.execute("CREATE TABLE IF NOT EXISTS mart.fact_market_event_classification (event_id VARCHAR, standardized_title VARCHAR, impact_scope VARCHAR, scope_evidence VARCHAR, driver_category VARCHAR, driver_evidence VARCHAR, impact_direction VARCHAR, direction_evidence VARCHAR, matched_entities VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
        connection.execute("INSERT INTO mart.dim_event_source SELECT COALESCE((SELECT MAX(event_source_id) FROM mart.dim_event_source), 0) + 1, 'opendart_disclosure', 'OpenDART Disclosure', 'disclosure', CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) WHERE NOT EXISTS (SELECT 1 FROM mart.dim_event_source WHERE event_source_code = 'opendart_disclosure')", [loaded_at, loaded_at])
        for silver_path in silver_result["silver_paths"]:
            connection.execute("INSERT INTO mart.dim_stock SELECT COALESCE((SELECT MAX(stock_id) FROM mart.dim_stock), 0) + ROW_NUMBER() OVER (ORDER BY src.stock_code), src.stock_code, COALESCE(src.corp_name, src.stock_code), NULL, NULL, NULL, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src WHERE src.stock_code IS NOT NULL AND NOT EXISTS (SELECT 1 FROM mart.dim_stock AS dim WHERE dim.stock_code = src.stock_code)", [loaded_at, loaded_at, silver_path])
            connection.execute("INSERT INTO mart.fact_market_event SELECT 'opendart:' || src.disclosure_id, source_dim.event_source_id, stock.stock_id, CASE WHEN stock.stock_id IS NOT NULL THEN 'stock' ELSE 'unmatched' END, CAST(src.event_at AS TIMESTAMP), CAST(src.event_date AS DATE), src.report_name, src.remark, NULL, src.disclosure_id, TRUE, src.source, src.collection_id, CAST(src.collected_at AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src CROSS JOIN (SELECT event_source_id FROM mart.dim_event_source WHERE event_source_code = 'opendart_disclosure') AS source_dim LEFT JOIN mart.dim_stock AS stock ON src.stock_code = stock.stock_code WHERE NOT EXISTS (SELECT 1 FROM mart.fact_market_event AS fact WHERE fact.event_id = 'opendart:' || src.disclosure_id AND COALESCE(fact.stock_id, -1) = COALESCE(stock.stock_id, -1) AND fact.event_scope = CASE WHEN stock.stock_id IS NOT NULL THEN 'stock' ELSE 'unmatched' END)", [loaded_at, silver_path])
        connection.execute("CREATE OR REPLACE VIEW serving.v_stock_event_timeline AS SELECT stock.stock_code, stock.stock_name, source_dim.event_source_code, source_dim.event_source_name, source_dim.event_source_type, event.event_id, event.event_scope, event.event_at, event.event_date, event.event_title, event.event_summary, event.event_url, event.source_record_id, event.is_main_event, event.source, event.collection_id, event.collected_at, event.processed_at, classification.standardized_title, classification.impact_scope, classification.scope_evidence, classification.driver_category, classification.driver_evidence, classification.impact_direction, classification.direction_evidence, classification.matched_entities FROM mart.fact_market_event AS event INNER JOIN mart.dim_event_source AS source_dim ON event.event_source_id = source_dim.event_source_id LEFT JOIN mart.dim_stock AS stock ON event.stock_id = stock.stock_id LEFT JOIN mart.fact_market_event_classification AS classification ON event.event_id = classification.event_id")
    return {"collection_id": silver_result["collection_id"], "bgn_de": silver_result["bgn_de"], "end_de": silver_result["end_de"], "page_no": silver_result["page_no"], "disclosure_count": silver_result["disclosure_count"], "mart_path": str(mart_path)}


# 여러 후보 환경변수 중 첫 번째 유효한 값을 읽고, 없으면 지정한 에러를 발생시킨다.
def _read_required_env(error_message, *env_names):
    for env_name in env_names:
        env_value = os.environ.get(env_name)
        if env_value:
            return env_value
    raise ValueError(error_message)


# OpenDART 기본 URL을 환경변수에서 읽고, 없으면 기본값을 사용한다.
def _read_base_url():
    return (
        os.environ.get("OPENDART_BASE_URL")
        or DEFAULT_OPENDART_BASE_URL
    ).rstrip("/")


# 수집 시각 기준으로 bronze 저장 경로에 사용할 collection_id를 만든다.
def _build_collection_id(collected_at):
    return f"{collected_at.format('YYYYMMDDTHHmmss')}_{uuid.uuid4().hex[:8]}"


# OpenDART 공시 목록 호출에 필요한 파라미터를 기본값과 함께 구성한다.
def _build_request_params(query_params, api_key):
    return {
        "crtfc_key": api_key,
        "bgn_de": str(query_params["bgn_de"]),
        "end_de": str(query_params["end_de"]),
        "last_reprt_at": str(query_params.get("last_reprt_at") or DEFAULT_LAST_REPRT_AT),
        "sort": str(query_params.get("sort") or DEFAULT_SORT),
        "sort_mth": str(query_params.get("sort_mth") or DEFAULT_SORT_MTH),
        "page_count": str(query_params.get("page_count") or DEFAULT_PAGE_COUNT),
    }


# 특정 페이지의 OpenDART 공시 목록 응답 1건을 요청하고 검증된 결과만 반환한다.
def _request_page_response(base_url, request_params, page_no):
    page_request = _build_page_request(base_url, request_params, page_no)
    for attempt in range(OPENDART_REQUEST_MAX_ATTEMPTS):
        try:
            page_response = _request_json_once("GET", page_request)
            _validate_response_body(page_response["body"])
            return page_response
        except RuntimeError as exc:
            if attempt == OPENDART_REQUEST_MAX_ATTEMPTS - 1:
                raise RuntimeError(f"OpenDART 공시목록 조회 실패: {exc}") from exc
            time.sleep(_build_backoff_seconds(attempt))


# 특정 페이지 번호를 포함한 OpenDART 요청 설정을 만든다.
def _build_page_request(base_url, request_params, page_no):
    page_params = _build_page_params(request_params, page_no)
    return {
        "url": _build_request_url(base_url, page_params),
        "headers": _build_json_headers(),
        "params": page_params,
    }


# OpenDART 공통 요청 파라미터에 페이지 번호를 추가한 dict를 만든다.
def _build_page_params(request_params, page_no):
    return {**request_params, "page_no": str(page_no)}


# OpenDART 공시 목록 요청 URL을 쿼리스트링까지 포함해 완성한다.
def _build_request_url(base_url, request_params):
    query_string = parse.urlencode(request_params)
    return f"{base_url}{OPENDART_DISCLOSURE_LIST_ENDPOINT}?{query_string}"


# OpenDART JSON API 호출에서 공통으로 사용하는 기본 헤더를 만든다.
def _build_json_headers():
    return {
        "Accept": "application/json",
        "User-Agent": OPENDART_USER_AGENT,
    }


# 주어진 요청 설정으로 OpenDART API를 재시도와 함께 호출하고 JSON 응답을 반환한다.
def _request_json(method, request_config, error_context):
    for attempt in range(OPENDART_REQUEST_MAX_ATTEMPTS):
        try:
            return _request_json_once(method, request_config)
        except RuntimeError as exc:
            if attempt == OPENDART_REQUEST_MAX_ATTEMPTS - 1:
                raise RuntimeError(f"{error_context}: {exc}") from exc
            time.sleep(_build_backoff_seconds(attempt))


# OpenDART API를 한 번 호출하고 JSON 응답을 표준 dict 형태로 변환한다.
def _request_json_once(method, request_config):
    try:
        with request.urlopen(
            _build_http_request(method, request_config),
            timeout=30,
        ) as response:
            return _read_response(response)
    except error.HTTPError as exc:
        _raise_http_error(exc)
    except error.URLError as exc:
        raise RuntimeError(f"reason={exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("JSON 응답 파싱 실패") from exc


# 재시도 간격을 점진적으로 늘리기 위한 backoff 초를 계산한다.
def _build_backoff_seconds(attempt):
    return attempt + 1 + OPENDART_REQUEST_INTERVAL_SECONDS


# urllib 요청 객체를 만들어 OpenDART API 호출에 넘긴다.
def _build_http_request(method, request_config):
    return request.Request(
        url=request_config["url"],
        headers=request_config["headers"],
        method=method,
    )


# HTTP 오류 응답을 읽어 상태 코드와 body를 포함한 RuntimeError로 변환한다.
def _raise_http_error(exc):
    error_body = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(f"status={exc.code}, body={error_body}") from exc


# urllib 응답 객체를 status, headers, body 구조의 dict로 정리한다.
def _read_response(response):
    return {
        "status_code": response.status,
        "headers": dict(response.headers.items()),
        "body": json.loads(response.read().decode("utf-8")),
    }


# OpenDART 응답 body의 상태 코드가 정상 범위인지 확인한다.
def _validate_response_body(response_body):
    status = str(response_body.get("status") or "")
    if status in VALID_OPENDART_STATUS_CODES:
        return
    message = response_body.get("message")
    raise RuntimeError(f"status={status}, message={message}")


# 첫 페이지 응답에서 total_page 값을 읽고, 없으면 1페이지로 간주한다.
def _read_total_page(response_body):
    total_page = response_body.get("total_page")
    return int(total_page or 1)


# page 1 응답을 기준으로 OpenDART page manifest 전체 구조를 조립한다.
def _build_page_manifest(base_url, collected_at, collection_id, request_params, first_page_response):
    response_body = first_page_response["body"]
    return {
        "source": OPENDART_SOURCE,
        "endpoint": f"{base_url}{OPENDART_DISCLOSURE_LIST_ENDPOINT}",
        "collected_at": collected_at.to_iso8601_string(),
        "collection_id": collection_id,
        "request": {"params": _build_redacted_request_params(request_params)},
        "response": _build_manifest_response(response_body),
    }


# bronze에 저장할 OpenDART 단일 페이지 raw payload 전체 구조를 조립한다.
def _build_raw_payload(base_url, page_request, request_params, page_response):
    return {
        "source": OPENDART_SOURCE,
        "endpoint": f"{base_url}{OPENDART_DISCLOSURE_LIST_ENDPOINT}",
        "collected_at": page_request["collected_at"],
        "collection_id": page_request["collection_id"],
        "request": {"params": _build_redacted_request_params(request_params)},
        "response": page_response,
    }


# bronze 저장용 요청 파라미터에서 인증키를 마스킹한 형태를 만든다.
def _build_redacted_request_params(request_params):
    return {
        **request_params,
        "crtfc_key": "***redacted***",
    }


# 첫 페이지 응답에서 manifest 저장용 최소 response 블록을 정리한다.
def _build_manifest_response(response_body):
    return {
        "status": response_body.get("status"),
        "message": response_body.get("message"),
        "total_count": response_body.get("total_count"),
        "total_page": _read_total_page(response_body),
    }


# raw payload를 bronze 경로에 JSON 파일로 저장하고 저장 경로를 반환한다.
def _write_bronze_payload(raw_payload):
    bronze_path = _build_bronze_path(raw_payload)
    with bronze_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, ensure_ascii=False, indent=2)
    return bronze_path


# bronze 파일이 저장될 디렉터리를 만들고 최종 data.json 경로를 반환한다.
def _build_bronze_path(raw_payload):
    params = raw_payload["request"]["params"]
    bronze_dir = (
        LOCAL_S3_ROOT
        / "bronze"
        / OPENDART_SOURCE
        / f"bgn_de={params['bgn_de']}"
        / f"end_de={params['end_de']}"
        / f"collection_id={raw_payload['collection_id']}"
        / f"page_no={params['page_no']}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir / "data.json"


# bronze 저장 결과를 다음 단계에서 쓰기 쉬운 최소 정보 dict로 변환한다.
def _build_write_result(raw_payload, bronze_path):
    params = raw_payload["request"]["params"]
    return {
        "collection_id": raw_payload["collection_id"],
        "bgn_de": params["bgn_de"],
        "end_de": params["end_de"],
        "page_no": params["page_no"],
        "bronze_path": str(bronze_path),
    }
