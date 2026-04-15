import json
import os
import uuid
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib import error, request

import pendulum


LOCAL_S3_ROOT = Path("/opt/airflow/s3")
MK_RSS_FEEDS = {
    "mk_economy": {
        "label": "economy",
        "name": "Maeil Business Newspaper Economy",
        "url": "https://www.mk.co.kr/rss/30100041/",
    },
    "mk_stock": {
        "label": "stock",
        "name": "Maeil Business Newspaper Stock",
        "url": "https://www.mk.co.kr/rss/50200011/",
    },
    "mk_corporate_management": {
        "label": "corporate_management",
        "name": "Maeil Business Newspaper Corporate Management",
        "url": "https://www.mk.co.kr/rss/50100032/",
    },
    "mk_international": {
        "label": "international",
        "name": "Maeil Business Newspaper International",
        "url": "https://www.mk.co.kr/rss/30300018/",
    },
}
IMPORTANT_EVENT_TITLE_KEYWORDS = [
    "금리",
    "물가",
    "cpi",
    "환율",
    "유가",
    "반도체",
    "수출",
    "실적",
    "가이던스",
    "관세",
    "지정학",
    "기준금리",
    "연준",
    "boj",
    "boe",
    "경기침체",
]


def _strip_namespace(tag_name):
    if "}" in tag_name:
        return tag_name.split("}", 1)[1]

    if ":" in tag_name:
        return tag_name.split(":", 1)[1]

    return tag_name


def _normalize_text(value):
    if value is None:
        return None

    normalized = " ".join(value.split())
    return normalized or None


def _serialize_element(element):
    text_value = _normalize_text("".join(element.itertext()))
    payload = {
        "text": text_value,
    }

    if element.attrib:
        payload["attributes"] = dict(element.attrib)

    child_tags = [_strip_namespace(child.tag) for child in list(element)]
    if child_tags:
        payload["child_tags"] = child_tags

    if list(payload.keys()) == ["text"]:
        return text_value

    return payload


def _collect_direct_fields(parent_element, excluded_tags=None):
    excluded_tags = excluded_tags or set()
    fields = {}

    for child in list(parent_element):
        tag_name = _strip_namespace(child.tag)
        if tag_name in excluded_tags:
            continue

        serialized_value = _serialize_element(child)

        if tag_name in fields:
            if not isinstance(fields[tag_name], list):
                fields[tag_name] = [fields[tag_name]]
            fields[tag_name].append(serialized_value)
            continue

        fields[tag_name] = serialized_value

    return fields


def _find_channel(root):
    for element in root.iter():
        if _strip_namespace(element.tag) == "channel":
            return element

    raise ValueError("RSS channel element를 찾지 못했습니다.")


def _match_title_keywords(title):
    if not title:
        return []

    normalized_title = title.lower()
    return [
        keyword
        for keyword in IMPORTANT_EVENT_TITLE_KEYWORDS
        if keyword.lower() in normalized_title
    ]


def _parse_published_at(pub_date):
    if not pub_date:
        return None

    try:
        return parsedate_to_datetime(pub_date).isoformat()
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


def collect_mk_rss_feed_to_bronze(source_feed):
    if source_feed not in MK_RSS_FEEDS:
        raise ValueError(f"지원하지 않는 source_feed 입니다: {source_feed}")

    feed_config = MK_RSS_FEEDS[source_feed]
    request_headers = {
        "Accept": "application/rss+xml, application/xml, text/xml",
        "User-Agent": "stock-signal-pipeline/mk-rss-ingestion",
    }

    try:
        with request.urlopen(
            request.Request(feed_config["url"], headers=request_headers),
            timeout=30,
        ) as response:
            response_bytes = response.read()
            response_charset = response.headers.get_content_charset() or "utf-8"
            response_text = response_bytes.decode(response_charset, errors="replace")
            response_headers = dict(response.headers.items())
            status_code = response.status
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"MK RSS 요청 실패: source_feed={source_feed}, status={exc.code}, body={error_body}",
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(
            f"MK RSS 연결 실패: source_feed={source_feed}, reason={exc.reason}",
        ) from exc

    try:
        root = ET.fromstring(response_bytes)
    except ET.ParseError as exc:
        raise RuntimeError(
            f"MK RSS XML 파싱 실패: source_feed={source_feed}, error={exc}",
        ) from exc

    channel = _find_channel(root)
    channel_fields = _collect_direct_fields(channel, excluded_tags={"item"})

    parsed_items = []
    for item in list(channel):
        if _strip_namespace(item.tag) != "item":
            continue

        item_fields = _collect_direct_fields(item)
        title = item_fields.get("title")
        matched_keywords = _match_title_keywords(title)

        categories = item_fields.get("category", [])
        if categories is None:
            categories = []
        if not isinstance(categories, list):
            categories = [categories]

        parsed_items.append(
            {
                "source_feed": source_feed,
                "source_feed_name": feed_config["name"],
                "source_feed_url": feed_config["url"],
                "raw_item_xml": ET.tostring(item, encoding="unicode"),
                "title": title,
                "link": item_fields.get("link"),
                "description": item_fields.get("description"),
                "guid": item_fields.get("guid"),
                "author": item_fields.get("author"),
                "pub_date": item_fields.get("pubDate"),
                "published_at": _parse_published_at(item_fields.get("pubDate")),
                "categories": categories,
                "matched_keywords": matched_keywords,
                "is_important_candidate": bool(matched_keywords),
                "all_fields": item_fields,
            }
        )

    collected_at = pendulum.now("Asia/Seoul")
    collected_date = collected_at.format("YYYY-MM-DD")
    collection_id = f"{collected_at.format('YYYYMMDDTHHmmss')}_{uuid.uuid4().hex[:8]}"

    raw_payload = {
        "source": "mk_rss",
        "source_feed": source_feed,
        "source_feed_name": feed_config["name"],
        "source_feed_label": feed_config["label"],
        "source_feed_url": feed_config["url"],
        "collected_at": collected_at.to_iso8601_string(),
        "collection_id": collection_id,
        "request": {
            "headers": request_headers,
        },
        "response": {
            "status_code": status_code,
            "headers": response_headers,
            "raw_xml": response_text,
        },
        "channel": channel_fields,
        "items": parsed_items,
    }

    bronze_dir = (
        LOCAL_S3_ROOT
        / "bronze"
        / "mk_rss_raw"
        / f"source_feed={source_feed}"
        / f"collected_date={collected_date}"
        / f"collection_id={collection_id}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)

    bronze_path = bronze_dir / "data.json"
    with bronze_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, ensure_ascii=False, indent=2)

    important_candidate_count = sum(
        1 for item in parsed_items if item["is_important_candidate"]
    )

    return {
        "status": "success",
        "source_feed": source_feed,
        "item_count": len(parsed_items),
        "important_candidate_count": important_candidate_count,
        "bronze_path": str(bronze_path),
    }


def should_include_mk_international_feed():
    value = os.environ.get("STOCK_SIGNAL_MK_RSS_INCLUDE_INTERNATIONAL", "false")
    return value.strip().lower() in {"1", "true", "yes", "y"}
