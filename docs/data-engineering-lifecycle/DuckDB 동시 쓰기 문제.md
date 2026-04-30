# 1. 문제 상황

- Airflow에서 `write_silver_to_mart` task가 실행될 때 DuckDB mart 파일(`/opt/airflow/s3/mart/stock_signal.duckdb`)에 lock을 잡지 못해 실패했다.
- 도커 로그에는 `IO Error: Could not set lock on file ... Conflicting lock is held`가 찍혔고, 다른 Python 프로세스가 이미 같은 DuckDB 파일을 사용 중이라고 나온다.
- 즉 raw, bronze, silver 단계는 정상 처리됐더라도, 여러 DAG/task가 같은 mart DuckDB 파일에 동시에 쓰려고 하면 mart 적재 단계에서 파이프라인이 실패할 수 있다.

# 2. 문제 원인

- DuckDB는 여러 프로세스가 쓰기 작업을 할 수 없음[(DuckDB 동시성에 대한 이해)](<../reference/DuckDB 동시성에 대한 이해.md>)
  - → ∵ DuckDB는 서버가 없는 embedded OLAP DB이므로[(DuckDB의 구조)](<../reference/DuckDB의 구조.md>)
- INSERT만 수행하는 append-only라도 파일을 열 때 lock이 걸림[(DuckDB는 왜 append-only INSERT에서도 lock이 걸리는가?)](<../reference/DuckDB는 왜 append-only INSERT에서도 lock이 걸리는가?.md>)
- 현재 프로젝트에서 `여러 프로세스`는 DAG 자체가 아니라, LocalExecutor가 task 실행을 위해 띄우는 별도 프로세스를 의미[(Airflow Dag에서 여러 프로세스의 의미)](<../reference/Airflow Dag에서 여러 프로세스의 의미.md>)

# 3. 문제 해결책

### A. Aiflow Pool

- 장점
  - DuckDB mart 파일에 쓰는 task의 동시 실행 수를 Airflow 설정으로 제한할 수 있다.
  - 기존 DAG 구조를 크게 바꾸지 않고 적용할 수 있다.
- 한계
  - DuckDB의 동시 쓰기 제약 자체를 해결하는 것은 아니다.
  - Pool 설정이 누락된 task가 있으면 같은 문제가 다시 발생할 수 있다.
  - 처리량이 낮아진다.
  - task 이외의 프로세스에서 쓰기 작업을 시도하는 경우 문제가 다시 발생할 수 있다.
- 이 프로젝트에서 적합성
  - 현재 구조를 유지하면서 mart 적재 실패를 줄이는 가장 작은 변경이다.

```Python
# Airflow UI 또는 CLI에서 pool을 먼저 만든다.
# pool name: duckdb_mart_write_pool
# slots: 1
# description: DuckDB mart 파일 쓰기 task를 한 번에 1개만 실행

# 각 DAG의 DuckDB mart 적재 task에 같은 pool을 지정한다.
@task(
    execution_timeout=timedelta(seconds=50),
    pool="duckdb_mart_write_pool",
)
def write_silver_to_mart(silver_result):
    return write_stock_price_silver_to_mart(silver_result)

# MK RSS처럼 timeout 기준이 다른 DAG도 pool 이름은 동일하게 맞춘다.
@task(
    execution_timeout=timedelta(minutes=3),
    pool="duckdb_mart_write_pool",
)
def write_silver_to_mart(silver_result):
    return write_mk_rss_silver_to_mart(silver_result)

```

### B. 처리용 Dag 구현

- 장점
  - mart 적재를 별도 DAG로 분리해 쓰기 시점을 더 명확하게 제어할 수 있다.
  - raw, bronze, silver 처리와 mart 적재 책임을 분리할 수 있다.
- 한계
  - DAG 간 의존성 관리가 추가로 필요하다.
  - 전체 파이프라인 구조가 지금보다 복잡해진다.

### C. 소스별 마트 생성

- 장점
  - 소스별로 DuckDB 파일을 나누면 같은 파일에 동시에 쓰는 상황을 줄일 수 있다.
  - 장애가 특정 소스의 mart 파일로 제한된다.
- 한계
  - 통합 조회를 한다면 파일을 다시 합치거나 연결하는 처리가 필요하다.
  - mart 관리 대상이 늘어난다.
  - 데이터 확장성이 부족하다, 즉 데이터 소스를 확장하면 하나의 마트에 여러 소스를 써야하는 상황이 온다.(ex. MK RSS, OpenDART)

### D. DuckDB의 postgres extension

- 장점
  - DuckDB에서 PostgreSQL 데이터를 조회하거나 연계할 수 있다.
  - 저장소를 PostgreSQL로 옮기면 동시 쓰기 제약을 PostgreSQL 쪽에서 처리할 수 있다.
- 한계
  - DuckDB 파일의 동시 쓰기 문제가 직접 해결되는 것은 아니다.
  - PostgreSQL 운영과 스키마 관리가 추가된다.
- 이 프로젝트에서 적합성
  - mart 저장소를 PostgreSQL로 전환할 계획이 있을 때 검토할 수 있다.

### E. 동시 쓰기가 되는 OLAP(ex. Clickhouse)

- 장점
  - 여러 task가 동시에 쓰는 구조를 DB가 직접 지원할 수 있다.
  - 데이터 규모나 사용자가 늘어나는 상황에 더 잘 대응할 수 있다.
- 한계
  - 인프라와 운영 복잡도가 커진다.
- 이 프로젝트에서 적합성
  - 지금 단계에서는 과한 변경이고, 동시 쓰기가 핵심 요구사항이 될 때 검토하는 편이 적절하다.

# 4. 문제 해결 및 한계

이번 문제는 `B. 처리용 Dag 구현`으로 해결하는 것이 적절하다.

- 선택 이유
  - 여러 source DAG가 같은 DuckDB mart 파일에 직접 쓰는 구조를 끊을 수 있다.
  - raw, bronze, silver 처리와 mart 적재 책임을 분리할 수 있다.
  - mart 적재 순서와 실패 재처리를 한 DAG에서 관리할 수 있다.
- 한계
  - DuckDB의 여러 프로세스 동시 쓰기 제약 자체가 사라지는 것은 아니다.
  - DAG 간 의존성 관리가 추가된다.
  - mart 적재 DAG가 실패하면 silver 이후 데이터가 mart에 늦게 반영될 수 있다.
- 구현 시 신경써야 할 점
  - mart 적재 DAG는 `max_active_runs=1`로 실행을 제한한다.
  - source DAG는 silver 산출물을 안정적으로 남기고, mart DAG는 그 산출물을 입력으로 사용한다.
  - mart 적재는 같은 입력을 다시 처리해도 결과가 깨지지 않도록 idempotent하게 만든다.
