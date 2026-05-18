# Stock Signal Pipeline

국내 주식 가격과 시장 뉴스 이벤트를 수집, 정제, 적재하고 FastAPI 웹 화면에서 조회하는 데이터 엔지니어링 포트폴리오 프로젝트입니다.

이 프로젝트는 단순 API 호출 예제가 아니라, 원천별 수집 주기와 복구 가능성 차이를 반영해 `raw -> bronze -> silver -> mart -> serving` 흐름을 만들고, Airflow orchestration 관점에서 DuckDB mart 쓰기 충돌과 재처리 문제를 다룹니다.

![Pipeline Architecture](docs/image/pipeline-architecture.png)

## 현재 구현 상태

- `Apache Airflow 3.1.8` LocalExecutor 기반으로 수집, 변환, mart 적재, 운영 점검 DAG를 구성했습니다.
- KIS 현재가와 MK RSS는 raw/bronze/silver 산출물과 단계별 품질 체크 결과를 남깁니다.
- KIS 현재가와 MK RSS의 silver 산출물은 manifest를 통해 추적하고, 별도 `load_silver_to_mart` DAG가 아직 적재되지 않은 파일만 DuckDB mart에 반영합니다.
- KIS 일봉 이력은 logical date 기준으로 날짜별 backfill이 가능하도록 별도 DAG에서 수집, bronze, silver, mart 적재를 순차 실행합니다.
- KIS 현재가와 MK RSS의 manifest 기반 mart 쓰기는 Airflow pool, OS file lock, transaction, 적재 완료 로그를 조합해 중복 적재와 write lock 위험을 줄입니다.
- MK RSS 뉴스 제목은 외부 AI 호출 없이 CSV 기반 knowledge graph와 규칙으로 시장 전체, 섹터, 기업 이벤트로 분류합니다.
- FastAPI 웹 앱은 DuckDB serving view를 read-only로 조회해 가격 차트와 이벤트 목록을 제공합니다.
- OpenDART DAG와 plugin 구조는 남아 있지만, 현재 DAG 첫 task에서 `AirflowSkipException`을 발생시켜 수집은 비활성화되어 있습니다.

## 기술 스택

| 영역          | 사용 기술                                                                         |
| ------------- | --------------------------------------------------------------------------------- |
| Orchestration | Apache Airflow 3.1.8, LocalExecutor                                               |
| Runtime       | Docker Compose, Airflow metadata Postgres                                         |
| Processing    | Python, pandas                                                                    |
| Storage       | Local S3-style directory, JSON, Parquet, DuckDB                                   |
| Quality/Ops   | 품질 체크 결과 파일, silver manifest, pending backlog 점검, Google Chat 실패 알림 |
| Serving       | DuckDB serving view, FastAPI                                                      |
| UI            | HTML, CSS, JavaScript                                                             |

## 데이터 레이어

| 레이어   | 역할                                          | 대표 산출물                                                           |
| -------- | --------------------------------------------- | --------------------------------------------------------------------- |
| Raw      | 외부 API/RSS 응답을 수집 task 반환값으로 보존 | KIS 응답 payload, MK RSS XML payload                                  |
| Bronze   | raw payload를 원형에 가깝게 JSON으로 저장     | `/opt/airflow/s3/bronze/.../data.json`                                |
| Silver   | 분석 가능한 parquet 스키마로 정제             | `silver_stock_price`, `silver_stock_price_daily`, `silver_news_event` |
| Manifest | 생성된 silver 파일 목록 기록                  | `/opt/airflow/s3/silver/_created_manifest/.../manifest.json`          |
| Mart     | DuckDB 테이블에 가격, 이벤트, 분류 결과 적재  | `mart.dim_stock`, `mart.fact_stock_price`, `mart.fact_market_event`   |
| Serving  | 웹/API 조회용 DuckDB view 제공                | `serving.v_stock_price_daily`, `serving.v_stock_event_timeline`       |
| Ops      | 품질 체크와 mart 적재 상태 관리               | `ops.mart_loaded_silver_file`, `ops/quality_check_result`             |

## 주요 DAG

| DAG                                         | 주기                                                 | 현재 역할                                                                 |
| ------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------- |
| `collect_kis_stock_price_raw`               | 평일 09:00-15:59 매분 생성, task에서 15:30 이후 skip | KIS 현재가 raw 수집, bronze/silver 생성, 품질 체크                        |
| `collect_kis_stock_price_daily_history_raw` | 평일 19:00, `catchup=True`                           | logical date 기준 KIS 일봉 수집, bronze/silver 생성, mart 적재            |
| `collect_mk_rss_raw`                        | 10분 단위                                            | MK 증권 RSS snapshot 수집, 기사 단위 silver 생성, 품질 체크               |
| `load_silver_to_mart`                       | 1분 단위                                             | manifest 기준 KIS 현재가와 MK RSS 미적재 silver 파일을 DuckDB mart에 적재 |
| `check_pipeline_health`                     | 1시간 단위                                           | 최근 품질 체크 실패와 오래된 pending silver backlog 점검                  |
| `recover_silver_to_mart`                    | 수동 실행                                            | 지정 기간 manifest를 다시 훑어 미적재 silver 파일 복구 적재               |
| `reproduce_duckdb_write_lock`               | 수동 실행                                            | DuckDB 동시 쓰기 lock 문제 재현                                           |
| `collect_opendart_raw`                      | 매일 19:00 등록, 현재 skip                           | OpenDART page manifest 기반 수집 구조만 유지                              |

## 수집 대상

현재 KIS 현재가와 KIS 일봉 DAG는 DAG 코드 안에 다음 3개 종목을 대상으로 고정해 두었습니다.

| 종목 코드 | 종목명      | 시장 구분 |
| --------- | ----------- | --------- |
| `108320`  | LX세미콘    | `J`       |
| `000660`  | SK 하이닉스 | `J`       |
| `042700`  | 한미반도체  | `J`       |

`airflow/.env.example`에는 `STOCK_SIGNAL_STOCK_CODE=005930` 값이 남아 있지만, 현재 수집 DAG는 이 Airflow Variable을 사용하지 않습니다. 다종목 설정을 환경변수나 Variable 기반으로 분리하는 작업은 아직 남아 있습니다.

## 데이터 흐름

```text
KIS current price DAG / MK RSS DAG
  -> raw payload
  -> raw quality check
  -> bronze JSON
  -> bronze quality check
  -> silver parquet
  -> silver quality check
  -> silver_created_manifest

load_silver_to_mart DAG
  -> ensure ops.mart_loaded_silver_file
  -> scan recent silver manifests
  -> exclude already loaded silver paths
  -> insert into DuckDB mart tables
  -> validate inserted mart rows
  -> mark silver path as loaded

KIS daily history DAG
  -> collect one logical_date
  -> bronze JSON
  -> silver parquet
  -> insert into DuckDB daily mart table
  -> create serving.v_stock_price_daily

FastAPI web app
  -> open DuckDB read-only
  -> query serving views first
  -> fallback to mart tables if serving view is missing
  -> expose /api/stock-prices and /api/stock-events
```

## Mart 모델

현재 DuckDB mart는 plugin 코드에서 필요한 테이블과 view를 생성합니다.

| Schema    | 객체                               | 설명                              |
| --------- | ---------------------------------- | --------------------------------- |
| `mart`    | `dim_stock`                        | 종목 차원                         |
| `mart`    | `fact_stock_price`                 | KIS 장중 현재가 snapshot fact     |
| `mart`    | `fact_stock_price_daily`           | KIS 일봉 fact                     |
| `mart`    | `dim_event_source`                 | 이벤트 원천 차원                  |
| `mart`    | `fact_market_event`                | MK RSS 기반 시장 이벤트 fact      |
| `mart`    | `fact_market_event_classification` | 뉴스 제목 분류 결과               |
| `ops`     | `mart_loaded_silver_file`          | mart 적재가 끝난 silver path 기록 |
| `serving` | `v_stock_price_timeline`           | 장중 현재가 조회 view             |
| `serving` | `v_stock_price_daily`              | 일봉 조회 view                    |
| `serving` | `v_stock_event_timeline`           | 이벤트 조회 view                  |

`ops.mart_loaded_silver_file`은 KIS 현재가와 MK RSS의 manifest 기반 적재에서 파일 단위 idempotency 기준으로 사용합니다.

## 설계 포인트

### 1. 원천별 orchestration 분리

KIS 현재가, KIS 일봉, MK RSS는 데이터 생성 주기와 재처리 가능성이 다릅니다.

- KIS 현재가: 장중 1분 snapshot입니다. 과거 snapshot을 API로 복구하기 어렵기 때문에 `catchup=False`이고, 15:30 이후 실행은 `short_circuit` task에서 skip합니다.
- KIS 일봉: 날짜별 재조회가 가능하므로 `catchup=True`를 사용하고, `{{ ds_nodash }}`를 조회 시작일/종료일로 넘깁니다.
- MK RSS: 최신 feed polling 성격이라 `catchup=False`이고, 10분 단위로 수집합니다.
- OpenDART: page manifest와 dynamic task mapping 구조는 있지만 현재 수집은 명시적으로 skip합니다.

### 2. 수집과 mart 적재 결합도 축소

KIS 현재가와 MK RSS 수집 DAG는 silver 파일 생성까지 담당하고 mart 적재는 별도 `load_silver_to_mart` DAG가 처리합니다. 이 구조는 source DAG가 DuckDB 파일에 직접 동시에 접근하는 상황을 줄이고, 수집 성공 후 mart 적재만 별도로 재시도하거나 복구할 수 있게 합니다.

KIS 일봉은 날짜 단위 backfill 대상이라 수집 DAG 안에서 mart 적재까지 이어집니다. 다만 현재 일봉 경로는 manifest와 `ops.mart_loaded_silver_file` 기반 복구 경로에 포함되어 있지 않습니다.

### 3. 파일 단위 idempotency

KIS 현재가와 MK RSS는 silver 생성 시 manifest를 남기고, mart 적재 후 `ops.mart_loaded_silver_file`에 `source`, `silver_path`, `loaded_at`, `dag_id`, `run_id`를 기록합니다.

동일 DAG run이 재시도되거나 recovery DAG를 다시 실행해도 이미 기록된 `silver_path`는 건너뛰므로 mart 중복 적재를 피할 수 있습니다.

### 4. DuckDB write lock 제어

DuckDB는 embedded database라 하나의 DB 파일에 여러 프로세스가 동시에 write connection을 열면 lock 문제가 발생할 수 있습니다. Airflow LocalExecutor는 task를 서로 다른 Python 프로세스에서 실행할 수 있으므로 mart writer 경로에 다음 장치를 둡니다.

- Airflow pool `duckdb_mart_writer` slot 1개로 mart writer task 직렬화
- `fcntl.flock` 기반 OS file lock으로 DuckDB write section 보호
- DuckDB transaction으로 mart insert와 loaded marker insert를 같은 작업 단위로 처리
- `ops.mart_loaded_silver_file` 검증으로 manifest의 silver path가 적재 완료 처리됐는지 확인

현재 OS file lock과 transaction은 주로 manifest 기반 loader/recovery 경로에서 사용합니다. KIS 일봉 mart 적재 경로는 Airflow pool로 직렬화되지만 공통 mart write helper와 완전히 같은 구조는 아닙니다.

### 5. 품질 체크와 운영 점검

KIS 현재가와 MK RSS 수집 DAG는 raw, bronze, silver 단계별 품질 체크 결과를 `/opt/airflow/s3/ops/quality_check_result` 아래에 저장합니다.

`check_pipeline_health` DAG는 다음 조건을 주기적으로 확인합니다.

- 최근 60분 품질 체크 실패가 있는지
- 최근 manifest와 `ops.mart_loaded_silver_file`을 비교했을 때 60분 이상 오래된 pending silver backlog가 있는지

실패가 발견되면 DAG를 실패시키고, 설정된 `GOOGLE_CHAT_WEBHOOK_URL`이 있으면 Google Chat 실패 콜백으로 알림을 보냅니다.

### 6. 결정적 뉴스 이벤트 분류

MK RSS 기사 제목은 외부 AI API를 호출하지 않고, `airflow/plugins/mk_kg_data`의 CSV knowledge graph와 정규식 규칙으로 분류합니다.

분류 결과는 silver parquet와 mart classification table에 함께 저장됩니다.

- `impact_scope`: `시장전체`, `섹터`, `기업`
- `driver_category`: 이벤트를 움직인 주요 요인
- `impact_direction`: `positive`, `negative`, `mixed`, `neutral`
- `*_evidence`: 분류 근거 키워드와 엔티티

이 방식은 같은 입력에 같은 출력을 반환하므로 비용, 재현성, 검증 가능성 측면에서 MVP 데이터 파이프라인에 적합합니다.

## 웹 화면

FastAPI 앱은 `../airflow/s3/mart/stock_signal.duckdb`를 컨테이너의 `/data/mart/stock_signal.duckdb`로 마운트하고, 기본적으로 read-only connection으로 조회합니다.

가격 API는 serving view가 있으면 먼저 사용하고, 없으면 mart table을 fallback으로 조회합니다.

<img src="docs/image/web-serving-view.png" alt="Stock Signal Web UI" width="900">

제공 API:

- `GET /api/stock-prices?stock_code=108320&range=1m`
- `GET /api/stock-events?stock_code=108320`
- `GET /health`

`/api/stock-prices`의 `range`는 `1d`, `5d`, `1m`, `6m`을 지원합니다. daily serving view가 있으면 일봉 기준으로 필터링하고, 없으면 장중 timeline 최근 240개를 반환합니다.

## 프로젝트 구조

```text
.
├── airflow
│   ├── dags                 # Airflow DAG 정의
│   ├── plugins              # 수집, 변환, 품질 체크, mart 적재 로직
│   ├── s3                   # 로컬 S3-style 데이터 레이크와 DuckDB mart
│   ├── logs                 # Airflow task 로그
│   └── docker-compose.yaml
├── docs                     # 설계 문서
├── web
│   ├── app.py               # FastAPI API 서버
│   ├── static               # 웹 UI
│   └── docker-compose.yaml
└── README.md
```

## 주요 엔지니어링 문제 해결

- 원천별 데이터 생성 주기와 복구 가능성이 달라 KIS 현재가, KIS 일봉, MK RSS, OpenDART의 schedule, catchup, skip 전략을 분리했습니다.
- 외부 API/RSS 실패가 전체 파이프라인 장애로 번지지 않도록 task 단위 retry, retry delay, execution timeout을 적용했습니다.
- 수집과 mart 적재가 강하게 결합되는 문제를 줄이기 위해 raw/bronze/silver/mart 레이어를 나누고, silver manifest로 파일 단위 lineage를 남겼습니다.
- DuckDB embedded DB의 동시 쓰기 lock 문제를 재현한 뒤 Airflow pool, OS file lock, transaction을 조합해 주요 mart write 경로를 직렬화했습니다.
- DAG 재시도와 recovery 실행에서 중복 적재가 발생하지 않도록 `ops.mart_loaded_silver_file` 기준의 파일 단위 idempotency를 구현했습니다.
- 짧은 장애와 긴 장애를 나눠 다루기 위해 hourly health check와 수동 recovery DAG를 함께 설계했습니다.
- 비정형 뉴스 제목을 서비스에 바로 노출하지 않고 KG와 규칙 기반 evidence로 시장 전체, 섹터, 기업 이벤트로 표준화했습니다.
- DuckDB serving view와 FastAPI를 연결해 파이프라인 산출물을 웹 화면에서 검증할 수 있게 했습니다.

## 현재 한계와 다음 개선 후보

- KIS 수집 대상 종목이 DAG 코드에 고정되어 있어 Airflow Variable이나 외부 설정 기반 다종목 확장이 필요합니다.
- KIS 일봉 mart 적재 경로는 manifest/loaded marker 기반 idempotency와 공통 OS lock helper 구조에 아직 완전히 합쳐져 있지 않습니다.
- OpenDART DAG는 등록되어 있지만 수집 task가 명시적으로 skip되어 있어 실제 공시 데이터 적재는 비활성화 상태입니다.
- DuckDB table/view DDL이 Python plugin 안에 있어, dbt 같은 SQL 모델 관리 도구로 분리할 여지가 있습니다.
- 로컬 S3-style 디렉터리를 사용하고 있어 실제 object storage 배포 시 credentials, 권한, I/O, retry 정책 검증이 필요합니다.
- Google Chat 알림은 실패 콜백 중심이며, 별도 metric 저장소나 SLA dashboard는 아직 없습니다.
- KG 기반 분류는 결정적이지만, 분류 품질을 검증하는 labeled test dataset과 평가 지표는 아직 부족합니다.
