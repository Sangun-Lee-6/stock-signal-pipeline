import json
import uuid
from pathlib import Path
from urllib import error, request

import pendulum


LOCAL_S3_ROOT = Path("/opt/airflow/s3")
MK_RSS_BRONZE_ROOT = LOCAL_S3_ROOT / "bronze" / "mk_rss"
MK_RSS_FEED_URL = "https://www.mk.co.kr/rss/50200011/"
MK_SOURCE = "mk_rss"
MK_SOURCE_FEED = "mk_stock"
MK_SOURCE_FEED_NAME = "Maeil Business Newspaper Stock"
MK_RSS_HEADERS = {
    "Accept": "application/rss+xml, application/xml, text/xml",
    "User-Agent": "stock-signal-pipeline/mk-rss-raw-ingestion",
}


# 매일경제 RSS 원본 응답 1건을 수집하고 bronze 저장용 raw payload를 반환한다.
def collect_mk_rss_raw():
    collected_at = pendulum.now("Asia/Seoul")
    collection_id = _build_collection_id(collected_at)
    rss_request = _build_rss_request()
    rss_response = _request_rss(rss_request, "MK RSS 수집 실패")
    return _build_raw_payload(collected_at, collection_id, rss_response)


# 수집한 MK RSS raw payload를 bronze 경로에 그대로 저장한다.
def write_mk_rss_raw_to_bronze(raw_payload):
    bronze_path = _write_bronze_payload(raw_payload)
    return _build_write_result(raw_payload, bronze_path)


# 수집 시각 기준으로 bronze 저장 경로에 사용할 collection_id를 만든다.
def _build_collection_id(collected_at):
    return f"{collected_at.format('YYYYMMDDTHHmmss')}_{uuid.uuid4().hex[:8]}"


# MK RSS 호출에 필요한 URL, 헤더 구성을 dict로 만든다.
def _build_rss_request():
    return {
        "url": MK_RSS_FEED_URL,
        "headers": MK_RSS_HEADERS,
    }


# 주어진 요청 설정으로 MK RSS를 호출하고 XML 응답을 표준 dict 형태로 변환한다.
def _request_rss(request_config, error_context):
    try:
        with request.urlopen(
            _build_http_request(request_config),
            timeout=30,
        ) as response:
            rss_response = _read_response(response)
            rss_body = rss_response["body"].lstrip()
            if not (
                rss_body.startswith("<?xml")
                or rss_body.startswith("<rss")
            ) or "<rss" not in rss_body:
                raise RuntimeError(f"{error_context}: RSS XML 응답이 아닙니다.")
            return rss_response
    except error.HTTPError as exc:
        _raise_http_error(exc, error_context)
    except error.URLError as exc:
        raise RuntimeError(f"{error_context}: reason={exc.reason}") from exc


# urllib 요청 객체를 만들어 RSS GET 호출에 넘긴다.
def _build_http_request(request_config):
    return request.Request(
        url=request_config["url"],
        headers=request_config["headers"],
        method="GET",
    )


# HTTP 오류 응답을 읽어 상태 코드와 body를 포함한 RuntimeError로 변환한다.
def _raise_http_error(exc, error_context):
    error_body = exc.read().decode("utf-8", errors="replace")
    raise RuntimeError(f"{error_context}: status={exc.code}, body={error_body}") from exc


# urllib 응답 객체를 status, headers, body 구조의 dict로 정리한다.
def _read_response(response):
    return {
        "status_code": response.status,
        "headers": dict(response.headers.items()),
        "body": response.read().decode("utf-8"),
    }


# bronze에 저장할 MK RSS raw payload 전체 구조를 조립한다.
def _build_raw_payload(collected_at, collection_id, rss_response):
    return {
        "source": MK_SOURCE,
        "source_feed": MK_SOURCE_FEED,
        "source_feed_name": MK_SOURCE_FEED_NAME,
        "source_feed_url": MK_RSS_FEED_URL,
        "collected_at": collected_at.to_iso8601_string(),
        "collection_id": collection_id,
        "response": rss_response,
    }


# raw payload를 bronze 경로에 JSON 파일로 저장하고 저장 경로를 반환한다.
def _write_bronze_payload(raw_payload):
    bronze_path = _build_bronze_path(raw_payload)
    with bronze_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, ensure_ascii=False, indent=2)
    return bronze_path


# bronze 파일이 저장될 디렉터리를 만들고 최종 data.json 경로를 반환한다.
def _build_bronze_path(raw_payload):
    collected_at = pendulum.parse(raw_payload["collected_at"])
    bronze_dir = (
        MK_RSS_BRONZE_ROOT
        / f"source_feed={raw_payload['source_feed']}"
        / f"collected_date={collected_at.format('YYYY-MM-DD')}"
        / f"collection_id={raw_payload['collection_id']}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir / "data.json"


# bronze 저장 결과를 다음 단계에서 쓰기 쉬운 최소 정보 dict로 변환한다.
def _build_write_result(raw_payload, bronze_path):
    return {
        "collection_id": raw_payload["collection_id"],
        "bronze_path": str(bronze_path),
    }
