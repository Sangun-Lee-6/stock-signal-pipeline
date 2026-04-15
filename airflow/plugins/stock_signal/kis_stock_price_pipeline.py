import json
import os
import uuid
from dataclasses import dataclass
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


@dataclass(frozen=True)
class KisCredentials:
    app_key: str
    app_secret: str


@dataclass(frozen=True)
class StockTarget:
    stock_code: str
    stock_name: str
    market_division_code: str = "J"


@dataclass(frozen=True)
class JsonHttpResponse:
    status_code: int
    headers: dict[str, str]
    body: dict[str, object]


LX_SEMICON = StockTarget(
    stock_code="108320",
    stock_name="LX세미콘",
)


class EnvironmentVariableKisSettings:
    def read_credentials(self) -> KisCredentials:
        app_key = self._read_first_nonempty(
            "STOCK_SIGNAL_KIS_OPEN_API_APP_KEY",
            "KIS_OPEN_API_APP_KEY",
        )
        app_secret = self._read_first_nonempty(
            "STOCK_SIGNAL_KIS_OPEN_API_APP_SECRET",
            "KIS_OPEN_API_APP_SECRET",
        )

        if not app_key:
            raise ValueError(
                "KIS_OPEN_API_APP_KEY 또는 STOCK_SIGNAL_KIS_OPEN_API_APP_KEY 환경변수가 비어 있습니다.",
            )

        if not app_secret:
            raise ValueError(
                "KIS_OPEN_API_APP_SECRET 또는 STOCK_SIGNAL_KIS_OPEN_API_APP_SECRET 환경변수가 비어 있습니다.",
            )

        return KisCredentials(app_key=app_key, app_secret=app_secret)

    def read_base_url(self) -> str:
        return (
            self._read_first_nonempty(
                "STOCK_SIGNAL_KIS_OPEN_API_BASE_URL",
                "KIS_OPEN_API_BASE_URL",
            )
            or DEFAULT_KIS_OPEN_API_BASE_URL
        ).rstrip("/")

    @staticmethod
    def _read_first_nonempty(*env_names: str) -> str | None:
        for env_name in env_names:
            env_value = os.environ.get(env_name)
            if env_value:
                return env_value

        return None


class KisOpenApiClient:
    def __init__(
        self,
        credentials: KisCredentials,
        base_url: str,
        user_agent: str = KIS_USER_AGENT,
    ):
        self.credentials = credentials
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent

    def issue_access_token(self) -> JsonHttpResponse:
        return self._request_json(
            method="POST",
            url=f"{self.base_url}{KIS_ACCESS_TOKEN_ENDPOINT}",
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            },
            body={
                "grant_type": "client_credentials",
                "appkey": self.credentials.app_key,
                "appsecret": self.credentials.app_secret,
            },
            error_context="KIS 접근 토큰 발급 실패",
        )

    def inquire_current_price(
        self,
        access_token: str,
        stock: StockTarget,
    ) -> JsonHttpResponse:
        query_params = {
            "fid_cond_mrkt_div_code": stock.market_division_code,
            "fid_input_iscd": stock.stock_code,
        }

        request_url = (
            f"{self.base_url}{KIS_DOMESTIC_STOCK_PRICE_ENDPOINT}"
            f"?{parse.urlencode(query_params)}"
        )

        return self._request_json(
            method="GET",
            url=request_url,
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
                "appkey": self.credentials.app_key,
                "appsecret": self.credentials.app_secret,
                "tr_id": KIS_CURRENT_PRICE_TR_ID,
                "custtype": "P",
                "User-Agent": self.user_agent,
            },
            error_context="KIS 주식현재가 조회 실패",
        )

    def _request_json(
        self,
        method: str,
        url: str,
        headers: dict[str, str],
        error_context: str,
        body: dict[str, object] | None = None,
    ) -> JsonHttpResponse:
        request_body = None
        if body is not None:
            request_body = json.dumps(body).encode("utf-8")

        try:
            with request.urlopen(
                request.Request(
                    url=url,
                    headers=headers,
                    data=request_body,
                    method=method,
                ),
                timeout=30,
            ) as response:
                response_text = response.read().decode("utf-8")
                response_body = json.loads(response_text)

                return JsonHttpResponse(
                    status_code=response.status,
                    headers=dict(response.headers.items()),
                    body=response_body,
                )
        except error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"{error_context}: status={exc.code}, body={error_body}",
            ) from exc
        except error.URLError as exc:
            raise RuntimeError(f"{error_context}: reason={exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"{error_context}: JSON 응답 파싱 실패") from exc


class BronzeJsonWriter:
    def __init__(self, root_dir: Path):
        self.root_dir = root_dir

    def write_stock_price_payload(
        self,
        stock: StockTarget,
        collected_at: pendulum.DateTime,
        collection_id: str,
        payload: dict[str, object],
    ) -> Path:
        bronze_dir = (
            self.root_dir
            / "bronze"
            / "kis_stock_price_raw"
            / f"stock_code={stock.stock_code}"
            / f"collected_date={collected_at.format('YYYY-MM-DD')}"
            / f"collection_id={collection_id}"
        )
        bronze_dir.mkdir(parents=True, exist_ok=True)

        bronze_path = bronze_dir / "data.json"
        with bronze_path.open("w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)

        return bronze_path


class KisStockPriceCollector:
    def __init__(
        self,
        settings: EnvironmentVariableKisSettings,
        writer: BronzeJsonWriter,
    ):
        self.settings = settings
        self.writer = writer

    def collect_to_bronze(self, stock: StockTarget) -> dict[str, object]:
        credentials = self.settings.read_credentials()
        client = KisOpenApiClient(
            credentials=credentials,
            base_url=self.settings.read_base_url(),
        )

        token_response = client.issue_access_token()
        access_token = self._extract_access_token(token_response.body)
        quote_response = client.inquire_current_price(
            access_token=access_token,
            stock=stock,
        )

        collected_at = pendulum.now("Asia/Seoul")
        collection_id = f"{collected_at.format('YYYYMMDDTHHmmss')}_{uuid.uuid4().hex[:8]}"

        raw_payload = self._build_raw_payload(
            stock=stock,
            client=client,
            collected_at=collected_at,
            collection_id=collection_id,
            token_response=token_response,
            quote_response=quote_response,
        )
        bronze_path = self.writer.write_stock_price_payload(
            stock=stock,
            collected_at=collected_at,
            collection_id=collection_id,
            payload=raw_payload,
        )

        self._validate_quote_response(quote_response.body)
        quote_output = quote_response.body.get("output")

        if not isinstance(quote_output, dict):
            raise RuntimeError("KIS 주식현재가 응답에 output 객체가 없습니다.")

        return {
            "collection_id": collection_id,
            "collected_at": raw_payload["collected_at"],
            "stock_code": stock.stock_code,
            "stock_name": stock.stock_name,
            "current_price": self._read_int(quote_output, "stck_prpr"),
            "price_change": self._read_int(quote_output, "prdy_vrss"),
            "price_change_rate": self._read_float(quote_output, "prdy_ctrt"),
            "bronze_path": str(bronze_path),
        }

    def _build_raw_payload(
        self,
        stock: StockTarget,
        client: KisOpenApiClient,
        collected_at: pendulum.DateTime,
        collection_id: str,
        token_response: JsonHttpResponse,
        quote_response: JsonHttpResponse,
    ) -> dict[str, object]:
        return {
            "source": "kis_open_api",
            "endpoint": f"{client.base_url}{KIS_DOMESTIC_STOCK_PRICE_ENDPOINT}",
            "collected_at": collected_at.to_iso8601_string(),
            "collection_id": collection_id,
            "stock": {
                "stock_code": stock.stock_code,
                "stock_name": stock.stock_name,
                "market_division_code": stock.market_division_code,
            },
            "authentication": {
                "token_endpoint": f"{client.base_url}{KIS_ACCESS_TOKEN_ENDPOINT}",
                "request": {
                    "headers": {
                        "Content-Type": "application/json; charset=UTF-8",
                        "Accept": "application/json",
                        "User-Agent": client.user_agent,
                    },
                    "body": {
                        "grant_type": "client_credentials",
                        "appkey": "***redacted***",
                        "appsecret": "***redacted***",
                    },
                },
                "response": {
                    "status_code": token_response.status_code,
                    "headers": token_response.headers,
                    "body": {
                        "token_type": token_response.body.get("token_type"),
                        "expires_in": token_response.body.get("expires_in"),
                        "access_token_token_expired": token_response.body.get(
                            "access_token_token_expired",
                        ),
                    },
                },
            },
            "request": {
                "headers": {
                    "Content-Type": "application/json; charset=UTF-8",
                    "Accept": "application/json",
                    "Authorization": "Bearer ***redacted***",
                    "appkey": "***redacted***",
                    "appsecret": "***redacted***",
                    "tr_id": KIS_CURRENT_PRICE_TR_ID,
                    "custtype": "P",
                    "User-Agent": client.user_agent,
                },
                "params": {
                    "fid_cond_mrkt_div_code": stock.market_division_code,
                    "fid_input_iscd": stock.stock_code,
                },
            },
            "response": {
                "status_code": quote_response.status_code,
                "headers": quote_response.headers,
                "body": quote_response.body,
            },
        }

    @staticmethod
    def _extract_access_token(token_body: dict[str, object]) -> str:
        access_token = token_body.get("access_token")

        if not access_token:
            raise RuntimeError(
                "KIS 접근 토큰 발급 응답에 access_token 이 없습니다.",
            )

        return str(access_token)

    @staticmethod
    def _validate_quote_response(response_body: dict[str, object]):
        response_code = response_body.get("rt_cd")
        response_message = response_body.get("msg1")
        output = response_body.get("output")

        if response_code != "0" or not isinstance(output, dict):
            raise RuntimeError(
                f"KIS 주식현재가 응답 오류: rt_cd={response_code}, msg1={response_message}",
            )

    @staticmethod
    def _read_int(output: dict[str, object], key: str) -> int | None:
        value = output.get(key)
        if value in {None, ""}:
            return None

        return int(str(value))

    @staticmethod
    def _read_float(output: dict[str, object], key: str) -> float | None:
        value = output.get(key)
        if value in {None, ""}:
            return None

        return float(str(value))


def collect_lx_semicon_price_to_bronze():
    collector = KisStockPriceCollector(
        settings=EnvironmentVariableKisSettings(),
        writer=BronzeJsonWriter(LOCAL_S3_ROOT),
    )

    return collector.collect_to_bronze(LX_SEMICON)
