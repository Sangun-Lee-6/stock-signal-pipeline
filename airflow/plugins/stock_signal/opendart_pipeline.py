import json
import os
import uuid
from pathlib import Path
from urllib import error, parse, request

import pendulum


LOCAL_S3_ROOT = Path("/opt/airflow/s3")
OPENDART_DISCLOSURE_LIST_ENDPOINT = "https://opendart.fss.or.kr/api/list.json"


def _read_opendart_api_key():
    return os.environ.get("STOCK_SIGNAL_OPEN_DART_API_KEY") or os.environ.get(
        "OPEN_DART_API_KEY"
    )


def collect_opendart_disclosures_to_bronze():
    api_key = _read_opendart_api_key()

    if not api_key:
        raise ValueError(
            "OPEN_DART_API_KEY 또는 STOCK_SIGNAL_OPEN_DART_API_KEY 환경변수가 비어 있습니다.",
        )

    end_date = os.environ.get(
        "STOCK_SIGNAL_OPEN_DART_END_DE",
        pendulum.now("Asia/Seoul").format("YYYYMMDD"),
    )
    begin_date = os.environ.get(
        "STOCK_SIGNAL_OPEN_DART_BGN_DE",
        pendulum.from_format(end_date, "YYYYMMDD", tz="Asia/Seoul")
        .subtract(days=1)
        .format("YYYYMMDD"),
    )

    request_params = {
        "crtfc_key": api_key,
        "bgn_de": begin_date,
        "end_de": end_date,
        "last_reprt_at": os.environ.get("STOCK_SIGNAL_OPEN_DART_LAST_REPRT_AT", "Y"),
        "sort": os.environ.get("STOCK_SIGNAL_OPEN_DART_SORT", "date"),
        "sort_mth": os.environ.get("STOCK_SIGNAL_OPEN_DART_SORT_MTH", "desc"),
        "page_no": os.environ.get("STOCK_SIGNAL_OPEN_DART_PAGE_NO", "1"),
        "page_count": os.environ.get("STOCK_SIGNAL_OPEN_DART_PAGE_COUNT", "100"),
    }

    optional_env_params = {
        "corp_code": "STOCK_SIGNAL_OPEN_DART_CORP_CODE",
        "pblntf_ty": "STOCK_SIGNAL_OPEN_DART_PBLNTF_TY",
        "pblntf_detail_ty": "STOCK_SIGNAL_OPEN_DART_PBLNTF_DETAIL_TY",
        "corp_cls": "STOCK_SIGNAL_OPEN_DART_CORP_CLS",
    }

    for param_name, env_name in optional_env_params.items():
        env_value = os.environ.get(env_name)
        if env_value:
            request_params[param_name] = env_value

    request_url = (
        f"{OPENDART_DISCLOSURE_LIST_ENDPOINT}?{parse.urlencode(request_params)}"
    )
    request_headers = {
        "Accept": "application/json",
        "User-Agent": "stock-signal-pipeline/opendart-raw-ingestion",
    }

    try:
        with request.urlopen(
            request.Request(request_url, headers=request_headers),
            timeout=30,
        ) as response:
            response_text = response.read().decode("utf-8")
            response_payload = json.loads(response_text)
            response_headers = dict(response.headers.items())
            status_code = response.status
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"OpenDART API 요청 실패: status={exc.code}, body={error_body}",
        ) from exc
    except error.URLError as exc:
        raise RuntimeError(f"OpenDART API 연결 실패: {exc.reason}") from exc

    collected_at = pendulum.now("Asia/Seoul")
    collected_date = collected_at.format("YYYY-MM-DD")
    collection_id = f"{collected_at.format('YYYYMMDDTHHmmss')}_{uuid.uuid4().hex[:8]}"

    stored_request_params = {
        **request_params,
        "crtfc_key": "***redacted***",
    }

    raw_payload = {
        "source": "opendart",
        "endpoint": OPENDART_DISCLOSURE_LIST_ENDPOINT,
        "collected_at": collected_at.to_iso8601_string(),
        "collection_id": collection_id,
        "request": {
            "headers": request_headers,
            "params": stored_request_params,
        },
        "response": {
            "status_code": status_code,
            "headers": response_headers,
            "body": response_payload,
        },
    }

    bronze_dir = (
        LOCAL_S3_ROOT
        / "bronze"
        / "opendart_disclosure_raw"
        / f"collected_date={collected_date}"
        / f"collection_id={collection_id}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)

    bronze_path = bronze_dir / "data.json"
    with bronze_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, ensure_ascii=False, indent=2)

    api_status = response_payload.get("status")
    api_message = response_payload.get("message")
    disclosure_list = response_payload.get("list", [])

    if api_status not in {"000", "013"}:
        raise RuntimeError(
            f"OpenDART API 응답 오류: status={api_status}, message={api_message}",
        )

    return {
        "collection_id": collection_id,
        "collected_at": raw_payload["collected_at"],
        "api_status": api_status,
        "api_message": api_message,
        "disclosure_count": len(disclosure_list),
        "bronze_path": str(bronze_path),
    }
