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


# bronze 저장 결과 dict를 읽어 RSS 기사 단위 silver parquet들로 저장하고 저장 결과를 반환한다.
def write_mk_rss_bronze_to_silver(bronze_result):
    import pandas as pd
    import xml.etree.ElementTree as ET
    from email.utils import parsedate_to_datetime

    bronze_path = Path(bronze_result["bronze_path"])
    raw_payload = json.loads(bronze_path.read_text(encoding="utf-8"))
    root = ET.fromstring(raw_payload["response"]["body"])
    channel = root.find("channel")
    processed_at = pendulum.now("Asia/Seoul").to_iso8601_string()
    silver_paths = []
    for item in channel.findall("item") if channel is not None else []:
        article_url = (item.findtext("link") or "").strip()
        article_id = ((item.findtext("no") or article_url.rstrip("/").split("/")[-1]).strip() or f"collection_{raw_payload['collection_id']}")
        published_at = parsedate_to_datetime((item.findtext("pubDate") or "").strip()).isoformat()
        published_date = pendulum.parse(published_at).format("YYYY-MM-DD")
        silver_path = LOCAL_S3_ROOT / "silver" / "silver_news_event" / f"source_feed={raw_payload['source_feed']}" / f"published_date={published_date}" / f"article_id={article_id}" / "data.parquet"
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"source": raw_payload["source"], "collection_id": raw_payload["collection_id"], "source_feed": raw_payload["source_feed"], "source_feed_name": raw_payload["source_feed_name"], "article_id": article_id, "article_url": article_url or None, "title": (item.findtext("title") or "").strip() or None, "description": (item.findtext("description") or "").strip() or None, "author": (item.findtext("author") or "").strip() or None, "category_names": [category.text.strip() for category in item.findall("category") if category.text and category.text.strip()] or None, "published_at": published_at, "published_date": published_date, "collected_at": raw_payload["collected_at"], "processed_at": processed_at}]).to_parquet(silver_path, index=False)
        silver_paths.append(str(silver_path))
    return {"collection_id": raw_payload["collection_id"], "source_feed": raw_payload["source_feed"], "article_count": len(silver_paths), "silver_paths": silver_paths}


# silver 저장 결과 dict를 읽어 RSS 기사들을 DuckDB mart 이벤트 테이블과 serving view에 적재한다.
def write_mk_rss_silver_to_mart(silver_result):
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
        connection.execute("INSERT INTO mart.dim_event_source SELECT COALESCE((SELECT MAX(event_source_id) FROM mart.dim_event_source), 0) + 1, 'mk_rss_news', 'MK RSS News', 'news', CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) WHERE NOT EXISTS (SELECT 1 FROM mart.dim_event_source WHERE event_source_code = 'mk_rss_news')", [loaded_at, loaded_at])
        for silver_path in silver_result["silver_paths"]:
            connection.execute("INSERT INTO mart.fact_market_event SELECT 'mk_rss:' || src.article_id, source_dim.event_source_id, NULL, 'market', CAST(src.published_at AS TIMESTAMP), CAST(src.published_date AS DATE), src.title, src.description, src.article_url, src.article_id, TRUE, src.source, src.collection_id, CAST(src.collected_at AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src CROSS JOIN (SELECT event_source_id FROM mart.dim_event_source WHERE event_source_code = 'mk_rss_news') AS source_dim WHERE NOT EXISTS (SELECT 1 FROM mart.fact_market_event AS fact WHERE fact.event_id = 'mk_rss:' || src.article_id AND fact.event_scope = 'market')", [loaded_at, silver_path])
        connection.execute("CREATE OR REPLACE VIEW serving.v_stock_event_timeline AS SELECT stock.stock_code, stock.stock_name, source_dim.event_source_code, source_dim.event_source_name, source_dim.event_source_type, event.event_id, event.event_scope, event.event_at, event.event_date, event.event_title, event.event_summary, event.event_url, event.source_record_id, event.is_main_event, event.source, event.collection_id, event.collected_at, event.processed_at FROM mart.fact_market_event AS event INNER JOIN mart.dim_event_source AS source_dim ON event.event_source_id = source_dim.event_source_id LEFT JOIN mart.dim_stock AS stock ON event.stock_id = stock.stock_id")
    return {"collection_id": silver_result["collection_id"], "source_feed": silver_result["source_feed"], "article_count": silver_result["article_count"], "mart_path": str(mart_path)}


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
