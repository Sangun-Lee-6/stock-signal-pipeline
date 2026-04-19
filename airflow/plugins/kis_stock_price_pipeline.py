import json
import os
import time
import uuid
from pathlib import Path
from urllib import error, parse, request

import pendulum


LOCAL_S3_ROOT = Path("/opt/airflow/s3")
DEFAULT_KIS_OPEN_API_BASE_URL = "https://openapi.koreainvestment.com:9443"
KIS_ACCESS_TOKEN_ENDPOINT = "/oauth2/tokenP"
KIS_DOMESTIC_STOCK_PRICE_ENDPOINT = (
    "/uapi/domestic-stock/v1/quotations/inquire-price"
)
KIS_CURRENT_PRICE_TR_ID = "FHKST01010100"
KIS_USER_AGENT = "stock-signal-pipeline/kis-stock-price-raw-ingestion"
TARGET_STOCK_CODE = "108320"
TARGET_STOCK_NAME = "LX세미콘"
TARGET_STOCK_MARKET_DIVISION_CODE = "J"
KIS_ACCESS_TOKEN_CACHE_PATH = Path("/tmp/kis_open_api_access_token.json")


# 현재 설정된 대상 종목의 현재가 raw 응답을 수집하고 bronze 저장용 payload를 반환한다.
def collect_stock_price_raw():
    app_key = _read_required_env(
        "KIS_OPEN_API_APP_KEY 환경변수가 비어 있습니다.",
        "KIS_OPEN_API_APP_KEY",
    )
    app_secret = _read_required_env(
        "KIS_OPEN_API_APP_SECRET 환경변수가 비어 있습니다.",
        "KIS_OPEN_API_APP_SECRET",
    )
    base_url = _read_base_url()
    token_request = _build_token_request(base_url, app_key, app_secret)
    access_token, token_response = _get_access_token(token_request)
    quote_request = _build_quote_request(base_url, app_key, app_secret, access_token)
    quote_response = _request_json("GET", quote_request, "KIS 주식현재가 조회 실패")
    quote_body = quote_response["body"]
    if str(quote_body.get("rt_cd")) != "0":
        raise RuntimeError(
            "KIS 주식현재가 조회 실패: "
            f"rt_cd={quote_body.get('rt_cd')}, "
            f"msg_cd={quote_body.get('msg_cd')}, "
            f"msg1={quote_body.get('msg1')}"
        )
    return _build_collected_stock_price_payload(
        token_request,
        token_response,
        quote_request,
        quote_response,
    )


# 수집한 raw payload를 bronze 경로에 저장하고 저장 결과만 반환한다.
def write_stock_price_raw_to_bronze(raw_payload):
    bronze_path = _write_bronze_payload(raw_payload)
    return _build_write_result(raw_payload, bronze_path)


# bronze 저장 결과 dict를 읽어 KIS silver parquet 1건을 저장하고 저장 결과를 반환한다.
def write_stock_price_bronze_to_silver(bronze_result):
    import pandas as pd

    bronze_path = Path(bronze_result["bronze_path"])
    raw_payload = json.loads(bronze_path.read_text(encoding="utf-8"))
    collected_at = pendulum.parse(raw_payload["collected_at"])
    processed_at = pendulum.now("Asia/Seoul").to_iso8601_string()
    output = raw_payload["response"]["body"]["output"]
    to_bool = lambda value: {"Y": True, "N": False}.get(value)
    to_number = lambda value: None if value in (None, "") else pd.to_numeric([value], errors="coerce")[0]
    silver_path = LOCAL_S3_ROOT / "silver" / "silver_stock_price" / f"stock_code={raw_payload['stock']['stock_code']}" / f"price_date={collected_at.format('YYYY-MM-DD')}" / f"collection_id={raw_payload['collection_id']}" / "data.parquet"
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"source": raw_payload["source"], "collection_id": raw_payload["collection_id"], "stock_code": raw_payload["stock"]["stock_code"], "stock_name": raw_payload["stock"]["stock_name"], "market_division_code": raw_payload["stock"]["market_division_code"], "market_name": output.get("rprs_mrkt_kor_name"), "industry_name": output.get("bstp_kor_isnm"), "price_at": raw_payload["collected_at"], "price_date": collected_at.format("YYYY-MM-DD"), "collected_at": raw_payload["collected_at"], "processed_at": processed_at, "current_price": to_number(output.get("stck_prpr")), "open_price": to_number(output.get("stck_oprc")), "high_price": to_number(output.get("stck_hgpr")), "low_price": to_number(output.get("stck_lwpr")), "base_price": to_number(output.get("stck_sdpr")), "change_value": to_number(output.get("prdy_vrss")), "change_rate": to_number(output.get("prdy_ctrt")), "volume_accumulated": to_number(output.get("acml_vol")), "trade_amount_accumulated": to_number(output.get("acml_tr_pbmn")), "per": to_number(output.get("per")), "pbr": to_number(output.get("pbr")), "eps": to_number(output.get("eps")), "bps": to_number(output.get("bps")), "is_trading_halted": to_bool(output.get("temp_stop_yn")), "is_credit_available": to_bool(output.get("crdt_able_yn"))}]).to_parquet(silver_path, index=False)
    return {"collection_id": raw_payload["collection_id"], "stock_code": raw_payload["stock"]["stock_code"], "stock_name": raw_payload["stock"]["stock_name"], "price_at": raw_payload["collected_at"], "silver_path": str(silver_path)}


# silver 저장 결과 dict를 읽어 KIS mart 테이블과 serving view에 적재하고 저장 결과를 반환한다.
def write_stock_price_silver_to_mart(silver_result):
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError("Airflow 실행 환경에 duckdb 패키지가 없습니다.") from exc
    silver_path = Path(silver_result["silver_path"])
    mart_path = LOCAL_S3_ROOT / "mart" / "stock_signal.duckdb"
    loaded_at = pendulum.now("Asia/Seoul").to_iso8601_string()
    mart_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(mart_path)) as connection:
        connection.execute("CREATE SCHEMA IF NOT EXISTS mart")
        connection.execute("CREATE SCHEMA IF NOT EXISTS serving")
        connection.execute("CREATE TABLE IF NOT EXISTS mart.dim_stock (stock_id BIGINT, stock_code VARCHAR, stock_name VARCHAR, market_division_code VARCHAR, market_name VARCHAR, industry_name VARCHAR, created_at TIMESTAMP, updated_at TIMESTAMP)")
        connection.execute("CREATE TABLE IF NOT EXISTS mart.fact_stock_price (stock_id BIGINT, price_at TIMESTAMP, price_date DATE, current_price DECIMAL(18,2), open_price DECIMAL(18,2), high_price DECIMAL(18,2), low_price DECIMAL(18,2), change_rate DECIMAL(9,4), volume_accumulated BIGINT, trade_amount_accumulated DECIMAL(18,2), source VARCHAR, collection_id VARCHAR, collected_at TIMESTAMP, processed_at TIMESTAMP)")
        connection.execute("INSERT INTO mart.dim_stock SELECT COALESCE((SELECT MAX(stock_id) FROM mart.dim_stock), 0) + ROW_NUMBER() OVER (ORDER BY src.stock_code), src.stock_code, src.stock_name, src.market_division_code, src.market_name, src.industry_name, CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM (SELECT DISTINCT stock_code, stock_name, market_division_code, market_name, industry_name FROM read_parquet(?)) AS src WHERE NOT EXISTS (SELECT 1 FROM mart.dim_stock AS dim WHERE dim.stock_code = src.stock_code)", [loaded_at, loaded_at, str(silver_path)])
        connection.execute("INSERT INTO mart.fact_stock_price SELECT dim.stock_id, CAST(src.price_at AS TIMESTAMP), CAST(src.price_date AS DATE), CAST(src.current_price AS DECIMAL(18,2)), CAST(src.open_price AS DECIMAL(18,2)), CAST(src.high_price AS DECIMAL(18,2)), CAST(src.low_price AS DECIMAL(18,2)), CAST(src.change_rate AS DECIMAL(9,4)), CAST(src.volume_accumulated AS BIGINT), CAST(src.trade_amount_accumulated AS DECIMAL(18,2)), src.source, src.collection_id, CAST(src.collected_at AS TIMESTAMP), CAST(? AS TIMESTAMP) FROM read_parquet(?) AS src INNER JOIN mart.dim_stock AS dim ON src.stock_code = dim.stock_code WHERE NOT EXISTS (SELECT 1 FROM mart.fact_stock_price AS fact WHERE fact.stock_id = dim.stock_id AND fact.price_at = CAST(src.price_at AS TIMESTAMP))", [loaded_at, str(silver_path)])
        connection.execute("CREATE OR REPLACE VIEW serving.v_stock_price_timeline AS SELECT stock.stock_code, stock.stock_name, price.price_at, price.price_date, price.current_price, price.open_price, price.high_price, price.low_price, price.change_rate, price.volume_accumulated, price.trade_amount_accumulated, price.source, price.collection_id, price.collected_at, price.processed_at FROM mart.fact_stock_price AS price INNER JOIN mart.dim_stock AS stock ON price.stock_id = stock.stock_id")
    return {"collection_id": silver_result["collection_id"], "stock_code": silver_result["stock_code"], "stock_name": silver_result["stock_name"], "price_at": silver_result["price_at"], "mart_path": str(mart_path)}


# 토큰 캐시를 포함해 현재 유효한 접근 토큰과 토큰 응답 메타데이터를 가져온다.
def _get_access_token(token_request):
    cached_token_payload, cached_expires_at = _read_cached_token()
    if _is_token_cache_valid(cached_token_payload, cached_expires_at):
        return _read_access_token_from_cache(cached_token_payload)
    return _issue_access_token(
        token_request,
        cached_token_payload,
        cached_expires_at,
    )


# 현재가 조회 응답을 raw payload 형태로 감싸 다음 단계에서 바로 저장할 수 있게 만든다.
def _build_collected_stock_price_payload(
    token_request,
    token_response,
    quote_request,
    quote_response,
):
    collected_at = pendulum.now("Asia/Seoul")
    collection_id = _build_collection_id(collected_at)
    return _build_raw_payload(
        collected_at,
        collection_id,
        token_request,
        token_response,
        quote_request,
        quote_response,
    )


# 디스크에 저장된 토큰 캐시를 읽고, 사용할 만료 시각까지 함께 반환한다.
def _read_cached_token():
    if not KIS_ACCESS_TOKEN_CACHE_PATH.exists():
        return None, None
    try:
        with KIS_ACCESS_TOKEN_CACHE_PATH.open("r", encoding="utf-8") as file:
            cached_token_payload = json.load(file)
        return cached_token_payload, _parse_cached_token_expired_at(cached_token_payload)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None, None


# 토큰 캐시에 저장된 만료 정보를 파싱해 비교 가능한 시각으로 변환한다.
def _parse_cached_token_expired_at(cached_token_payload):
    cached_expires_at_text = cached_token_payload.get("access_token_token_expired")
    if cached_expires_at_text:
        return pendulum.from_format(
            str(cached_expires_at_text),
            "YYYY-MM-DD HH:mm:ss",
            tz="Asia/Seoul",
        )
    if cached_token_payload.get("cached_at") and cached_token_payload.get("expires_in"):
        return pendulum.parse(str(cached_token_payload["cached_at"])).add(
            seconds=int(cached_token_payload["expires_in"]),
        )
    return None


# 캐시된 접근 토큰이 아직 안전하게 재사용 가능한 상태인지 판단한다.
def _is_token_cache_valid(cached_token_payload, cached_expires_at):
    return bool(
        cached_token_payload
        and cached_token_payload.get("access_token")
        and cached_expires_at
        and cached_expires_at > pendulum.now("Asia/Seoul").add(minutes=1)
    )


# 유효한 토큰 캐시를 access_token과 표준 응답 형태로 다시 조립한다.
def _read_access_token_from_cache(cached_token_payload):
    return (
        str(cached_token_payload["access_token"]),
        _build_cached_token_response(cached_token_payload),
    )


# 토큰 발급 API를 호출하고, 필요하면 제한 에러를 우회해 토큰을 확보한다.
def _issue_access_token(token_request, cached_token_payload, cached_expires_at):
    try:
        token_response = _request_json("POST", token_request, "KIS 접근 토큰 발급 실패")
    except RuntimeError as exc:
        if _should_use_cached_token_on_limit(exc, cached_token_payload, cached_expires_at):
            return _read_access_token_from_cache(cached_token_payload)
        if not _is_token_issue_rate_limited(exc):
            raise
        time.sleep(61)
        token_response = _request_json("POST", token_request, "KIS 접근 토큰 발급 실패")
    access_token = _extract_access_token(token_response["body"])
    _write_token_cache(access_token, token_response["body"])
    return access_token, token_response


# 토큰 발급 제한 오류지만 기존 캐시가 아직 유효한 경우 캐시 재사용 여부를 판단한다.
def _should_use_cached_token_on_limit(exc, cached_token_payload, cached_expires_at):
    return _is_token_issue_rate_limited(exc) and _is_token_cache_valid(
        cached_token_payload,
        cached_expires_at,
    )


# 토큰 발급 응답이 분당 호출 제한 오류인지 문자열 코드로 판별한다.
def _is_token_issue_rate_limited(exc):
    return "EGW00133" in str(exc)


# 캐시에서 읽은 토큰 메타데이터를 표준 token_response 구조로 만든다.
def _build_cached_token_response(cached_token_payload):
    return {
        "status_code": 200,
        "headers": {},
        "body": _build_token_response_body(cached_token_payload),
    }


# 새로 발급받은 토큰을 다음 호출에서 재사용할 수 있게 캐시 파일에 저장한다.
def _write_token_cache(access_token, token_body):
    with KIS_ACCESS_TOKEN_CACHE_PATH.open("w", encoding="utf-8") as file:
        json.dump(
            _build_token_cache_payload(access_token, token_body),
            file,
        )


# 토큰 캐시 파일에 저장할 최소 필드만 모아 직렬화용 dict로 만든다.
def _build_token_cache_payload(access_token, token_body):
    return {
        "access_token": access_token,
        "token_type": token_body.get("token_type"),
        "expires_in": token_body.get("expires_in"),
        "access_token_token_expired": token_body.get("access_token_token_expired"),
        "cached_at": pendulum.now("Asia/Seoul").to_iso8601_string(),
    }


# 여러 후보 환경변수 중 첫 번째 유효한 값을 읽고, 없으면 지정한 에러를 발생시킨다.
def _read_required_env(error_message, *env_names):
    for env_name in env_names:
        env_value = os.environ.get(env_name)
        if env_value:
            return env_value
    raise ValueError(error_message)


# KIS OpenAPI 기본 URL을 환경변수에서 읽고, 없으면 기본값을 사용한다.
def _read_base_url():
    return (
        os.environ.get("KIS_OPEN_API_BASE_URL")
        or DEFAULT_KIS_OPEN_API_BASE_URL
    ).rstrip("/")


# 접근 토큰 발급 API 호출에 필요한 URL, 헤더, 바디를 구성한다.
def _build_token_request(base_url, app_key, app_secret):
    return {
        "url": f"{base_url}{KIS_ACCESS_TOKEN_ENDPOINT}",
        "headers": _build_json_headers(),
        "body": {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret,
        },
    }


# 현재가 조회 API 호출에 필요한 URL, 헤더, 파라미터를 구성한다.
def _build_quote_request(base_url, app_key, app_secret, access_token):
    params = {
        "FID_COND_MRKT_DIV_CODE": TARGET_STOCK_MARKET_DIVISION_CODE,
        "FID_INPUT_ISCD": TARGET_STOCK_CODE,
    }
    return {
        "url": (
            f"{base_url}{KIS_DOMESTIC_STOCK_PRICE_ENDPOINT}"
            f"?{parse.urlencode(params)}"
        ),
        "headers": {
            **_build_json_headers(),
            "Authorization": f"Bearer {access_token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": KIS_CURRENT_PRICE_TR_ID,
            "custtype": "P",
        },
        "params": params,
    }


# KIS JSON API 호출에서 공통으로 사용하는 기본 헤더를 만든다.
def _build_json_headers():
    return {
        "Content-Type": "application/json; charset=UTF-8",
        "Accept": "application/json",
        "User-Agent": KIS_USER_AGENT,
    }


# 주어진 요청 설정으로 KIS API를 호출하고 JSON 응답을 표준 dict 형태로 변환한다.
def _request_json(method, request_config, error_context):
    try:
        with request.urlopen(
            _build_http_request(method, request_config),
            timeout=30,
        ) as response:
            return _read_response(response)
    except error.HTTPError as exc:
        _raise_http_error(exc, error_context)
    except error.URLError as exc:
        raise RuntimeError(f"{error_context}: reason={exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{error_context}: JSON 응답 파싱 실패") from exc


# urllib 요청 객체를 만들 때 body 직렬화까지 함께 처리한다.
def _build_http_request(method, request_config):
    request_body = request_config.get("body")
    request_data = None if request_body is None else json.dumps(request_body).encode("utf-8")
    return request.Request(
        url=request_config["url"],
        headers=request_config["headers"],
        data=request_data,
        method=method,
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
        "body": json.loads(response.read().decode("utf-8")),
    }


# 토큰 응답 바디에서 access_token 값을 꺼내고, 없으면 예외를 발생시킨다.
def _extract_access_token(token_body):
    access_token = token_body.get("access_token")
    if access_token:
        return str(access_token)
    raise RuntimeError("KIS 접근 토큰 발급 응답에 access_token 이 없습니다.")


# 수집 시각 기준으로 bronze 저장 경로에 사용할 collection_id를 만든다.
def _build_collection_id(collected_at):
    return f"{collected_at.format('YYYYMMDDTHHmmss')}_{uuid.uuid4().hex[:8]}"


# raw payload를 bronze 경로에 JSON 파일로 저장하고 저장 경로를 반환한다.
def _write_bronze_payload(raw_payload):
    bronze_path = _build_bronze_path(raw_payload)
    with bronze_path.open("w", encoding="utf-8") as file:
        json.dump(raw_payload, file, ensure_ascii=False, indent=2)
    return bronze_path


# bronze 파일이 저장될 디렉터리를 만들고 최종 data.json 경로를 반환한다.
def _build_bronze_path(raw_payload):
    collected_at = pendulum.parse(raw_payload["collected_at"])
    collection_id = raw_payload["collection_id"]
    stock_code = raw_payload["stock"]["stock_code"]
    bronze_dir = (
        LOCAL_S3_ROOT
        / "bronze"
        / "kis_open_api"
        / f"stock_code={stock_code}"
        / f"collected_date={collected_at.format('YYYY-MM-DD')}"
        / f"collection_id={collection_id}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir / "data.json"


# bronze에 저장할 KIS raw payload 전체 구조를 조립한다.
def _build_raw_payload(
    collected_at,
    collection_id,
    token_request,
    token_response,
    quote_request,
    quote_response,
):
    return {
        "source": "kis_open_api",
        "endpoint": quote_request["url"].split("?", 1)[0],
        "collected_at": collected_at.to_iso8601_string(),
        "collection_id": collection_id,
        "stock": _build_stock_payload(),
        "authentication": _build_auth_payload(token_request, token_response),
        "request": _build_request_payload(quote_request),
        "response": quote_response,
    }


# 현재 수집 대상 종목의 식별 정보를 payload용 dict로 만든다.
def _build_stock_payload():
    return {
        "stock_code": TARGET_STOCK_CODE,
        "stock_name": TARGET_STOCK_NAME,
        "market_division_code": TARGET_STOCK_MARKET_DIVISION_CODE,
    }


# 토큰 발급 요청과 응답을 마스킹된 형태로 authentication 블록에 담는다.
def _build_auth_payload(token_request, token_response):
    return {
        "token_endpoint": token_request["url"],
        "request": {
            "headers": token_request["headers"],
            "body": _build_redacted_token_body(),
        },
        "response": {
            "status_code": token_response["status_code"],
            "headers": token_response["headers"],
            "body": _build_token_response_body(token_response["body"]),
        },
    }


# bronze 저장용 토큰 요청 바디에서 민감정보를 제거한 형태를 만든다.
def _build_redacted_token_body():
    return {
        "grant_type": "client_credentials",
        "appkey": "***redacted***",
        "appsecret": "***redacted***",
    }


# 토큰 응답에서 보존할 메타데이터만 추려 저장용 dict로 만든다.
def _build_token_response_body(token_body):
    return {
        "token_type": token_body.get("token_type"),
        "expires_in": token_body.get("expires_in"),
        "access_token_token_expired": token_body.get("access_token_token_expired"),
    }


# 현재가 조회 요청 정보를 마스킹된 형태로 request 블록에 담는다.
def _build_request_payload(quote_request):
    return {
        "headers": _build_redacted_quote_headers(),
        "params": quote_request["params"],
    }


# bronze 저장용 현재가 조회 헤더에서 민감정보를 제거한 값을 만든다.
def _build_redacted_quote_headers():
    return {
        **_build_json_headers(),
        "Authorization": "Bearer ***redacted***",
        "appkey": "***redacted***",
        "appsecret": "***redacted***",
        "tr_id": KIS_CURRENT_PRICE_TR_ID,
        "custtype": "P",
    }


# bronze 저장 결과를 다음 단계에서 쓰기 쉬운 최소 정보 dict로 변환한다.
def _build_write_result(raw_payload, bronze_path):
    return {
        "collection_id": raw_payload["collection_id"],
        "collected_at": raw_payload["collected_at"],
        "stock_code": raw_payload["stock"]["stock_code"],
        "stock_name": raw_payload["stock"]["stock_name"],
        "bronze_path": str(bronze_path),
    }
