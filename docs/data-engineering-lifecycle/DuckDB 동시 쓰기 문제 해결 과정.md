# DuckDB 동시 쓰기 문제 해결 과정

## 1. 문제 상황

- 문제 상황 : `docs/data-engineering-lifecycle/DuckDB 동시 쓰기 문제.md`

- 문제 상황 요약
  - DuckDB는 embedded DB다.
  - 하나의 `.duckdb` 파일에 여러 프로세스가 동시에 write connection을 열 수 없다.
  - Airflow task는 여러 Python 프로세스로 실행될 수 있다.
  - 그래서 여러 DAG나 task가 같은 mart 파일에 쓰면 lock 충돌이 난다.

- 에러 로그
  ```text
  IO Error: Could not set lock on file "/opt/airflow/s3/mart/stock_signal.duckdb"
  Conflicting lock is held ...
  ```

## 2. 1차 해결 방향

- 따라서 이를 해결하기 위해 mart 쓰기용 Dag를 따로 생성했음
  - 데이터 수집 Dag들은 Silver까지만 적재
  - mart 쓰기용 Dag가 Silver에서 mart에 차례로 적재

- 데이터 수집 Dag

  ```text
  source DAG
  -> raw
  -> bronze
  -> silver
  -> silver_created_manifest(적재 목록)
  ```

- mart 쓰기용 Dag

  ```text
  load_silver_to_mart
  -> manifest 읽기
  -> 미적재 silver 파일 선별
  -> DuckDB mart 적재
  -> 적재 완료 기록
  ```

- 쓰기 작업을 하는 프로세스를 하나로 관리해서 DuckDB 동시 쓰기 문제를 해결

## 3. 문제 상황 : `>>`만으로는 부족

- 처음에는 DAG 의존성으로 순서를 잡으면 충분해 보였음

  ```text
  ensure_loaded_file_table
  -> load_kis_stock_price_silver
  -> load_mk_rss_silver
  ```

- 하지만 `>>`는 같은 DAG run 안의 순서만 보장하고, DuckDB write 전체를 직렬화하지는 못함

### 2가지 한계 상황

1. fan-out 구조에서는 동시에 실행 가능
   ```text
   start >> [writer_1, writer_2, writer_3]
   ```

- 이 구조는 `>>`를 사용해도 downstream writer들이 병렬 실행(`airflow/dags/reproduce_duckdb_write_lock_dag.py`로 상황 재현)
- 프로젝트 안에서도 `collect_opendart_raw_dag.py`는 현재 비활성화되어 있지만, 활성화되면 아래처럼 mapped mart writer를 만들 수 있음
- 에러 로그
  ```text
  IO Error: Could not set lock on file "/opt/airflow/s3/mart/duckdb_lock_repro.duckdb"
  Conflicting lock is held ...
  ```

2. 다른 DAG의 쓰기 작업은 막지 못한다.

- ex. recovery DAG, backfill DAG, manual run, retry task가 같은 DuckDB mart 파일에 쓸 수 있음
- 이 경우 한 DAG 안의 `>>` 순서는 의미가 없음

즉, `>>`는 DAG 내부 순서 제어이므로 프로젝트 전체의 DuckDB write lock을 제어하기엔 한계가 있음

## 4. 최종 보완 방향

- 따라서 프로젝트에서 DuckDB에 쓰기 작업을 하는 프로세스를 1개로 유지하기 위한 보호 장치를 추가
  - Airflow pool
  - OS lock
  - DuckDB transaction

## 5. Airflow Pool

- `duckdb_mart_writer` pool을 slot 1개로 설정
  - 이 pool을 사용하는 task는 Airflow scheduler 레벨에서 한 번에 하나만 실행됨

적용 대상:

- `load_silver_to_mart`의 mart writer task
- `recover_silver_to_mart`의 recovery writer task

## 6. OS Lock

- pool 밖의 코드가 DuckDB에 직접 접근할 수 있고 또 새 DAG가 추가될 때 pool 누락도 생길 수 있음
- 그래서 task 실행 시점에 OS lock을 한 번 더 설정하기

```text
/opt/airflow/s3/mart/.stock_signal.duckdb.write.lock
```

- DuckDB 파일 자체가 아니라 lock 전용 파일을 두고, 프로세스는 이 파일을 열고 file descriptor에 exclusive lock을 요청

```python
with open(lock_path, "a+") as lock_file:
    while timeout_not_reached:
        try:
            flock(lock_file.fileno(), LOCK_EX | LOCK_NB)
            break
        except BlockingIOError:
            sleep(poll_interval)

    try:
        yield
    finally:
        flock(lock_file.fileno(), LOCK_UN)
```

```text
OS lock 획득
  -> DuckDB connection 열기
  -> write 수행
  -> lock 해제
```

- DuckDB connection을 열기 전에 lock을 잡아야, 그래야 DuckDB 자체 lock 오류가 나기 전에 애플리케이션 레벨에서 먼저 막을 수 있음

### OS Lock을 사용하는데 Airflow Pool이 필요한 이유

- Airflow Pool은 정상적인 대기를 scheduler에 맡겨 불필요한 task 점유와 OS lock timeout 실패 감소

## 7. Transaction(중간 실패 시 원자성 보장)

- 파일 단위로 transaction을 열고 아래 작업을 같이 처리
  - mart table insert
  - `ops.mart_loaded_silver_file` 기록

- 이 구조 덕분에 중간 실패 후 재실행해도 판단 기준이 명확

## 8. Idempotency

- mart 적재 완료 여부는 DuckDB의 운영 테이블에 기록

```text
ops.mart_loaded_silver_file
```

- mart DAG는 이 테이블을 보고 이미 적재된 silver 파일을 건너뜀
- 즉, 재시도나 재실행이 발생해도 같은 silver 파일을 중복 적재하지 않음

## 9. 최종 흐름

```text
manifest 읽기
  -> ops.mart_loaded_silver_file 기준으로 미적재 파일 선별
  -> duckdb_mart_writer pool 진입
  -> OS lock 획득
  -> DuckDB connection 열기
  -> 파일 단위 transaction
  -> mart insert
  -> 적재 완료 기록
  -> commit
```

## 10. 설계 요소

| 설계 요소                                            | 의미                                |
| ---------------------------------------------------- | ----------------------------------- |
| `silver_created_manifest`                            | source DAG와 mart DAG의 결합도 제거 |
| `ops.mart_loaded_silver_file`                        | 파일 단위 idempotency 확보          |
| 같은 transaction 내 mart insert + loaded marker 기록 | 데이터와 소비 로그의 atomicity 확보 |
| lookback window                                      | 짧은 실패 후 자동 회복              |
| manual recovery DAG                                  | 긴 장애 후 수동 복구 경로           |
| `open_mart_write_lock`                               | 런타임 write critical section 보호  |
| Airflow pool slot=1                                  | scheduler-level write 직렬화        |
| 작은 transaction                                     | 실패 범위 축소 및 재처리 용이성     |
