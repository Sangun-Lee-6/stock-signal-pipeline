import fcntl
import time
from contextlib import contextmanager
from pathlib import Path


LOCAL_S3_ROOT = Path("/opt/airflow/s3")


@contextmanager # 이 함수를 with 문에서 사용할 수 있게 해줌
def open_mart_write_lock(timeout_seconds=30, poll_interval=0.2): # lock을 최대 30초 기다림, lock 획득 시도 간격은 0.2초
    """
    쓰기 작업 전에 OS 파일 Lock을 잡는 Context manager
    1. lock 파일 준비
    2. exclusive lock 시도
    3. 이미 누가 lock을 잡고 있다면 대기, timeout 넘으면 실패
    4. lock 획득 시 작업 수행
    5. with 블록 종료 시 lock 해제
    """
    lock_path = LOCAL_S3_ROOT / "mart" / ".stock_signal.duckdb.write.lock" # lock 대상 파일 경로(DuckDB 파일이 아니라 lock 전용 파일, DB 파일 자체에 직접 lock을 거는 것보다 운영상 명확)
    lock_path.parent.mkdir(parents=True, exist_ok=True) # lock 파일이 위치할 디렉터리 생성
    with lock_path.open("a+", encoding="utf-8") as lock_file: # lock 파일 열기, append 모드(파일 없으면 생성), +(읽기/쓰기 가능), 파일 내용이 중요한게 아니라 열린 fd가 중요, fcntl.flock()은 파일 경로가 아니라 열린 fd에 lock을 생성
        deadline = time.monotonic() + timeout_seconds # 기다리는 시간 계산(데드라인)
        # lock 획득 재시도 루프(타임아웃 조건에 걸릴 때까지, lock을 얻을 때까지 반복)
        while True:
            # exclusive lock 시도
            # 열린 파일의 fd 번호를 가져오고, OS에 파일 lock 요청
            # LOCK_EX : exclusive lock, 한 번에 하나의 프로세스만 잡을 수 있음
            # LOCK_NB : non-blocking 모드, lock을 잡을 수 없으면 즉시 예외 발생, blocking 모드였다면 lock이 풀릴 때까지 기다렸을 것
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break # lock 획득 후 while 루프 탈출
            except BlockingIOError:
                if time.monotonic() >= deadline: # 타임아웃 체크, 현재 시간이 데드라인을 넘었으면 예외 발생
                    raise TimeoutError(f"mart write lock 대기 시간이 초과되었습니다: {lock_path}")
                time.sleep(poll_interval) # 0.2초 대기 후 다시 while True로 돌아가서 lock 시도
        # yield 구문으로 with open_mart_write_lock() 블록 내부 코드 실행, lock이 잡힌 상태로 유지됨
        try:
            yield lock_path
        # with 블록이 끝나면 finally 구문으로 lock 해제, lock_file의 fd에 걸린 lock을 해제하여 다른 프로세스가 lock을 잡을 수 있도록 함
        # 블록에서 에러가 발생해도 finally는 실행되므로 lock이 확실히 해제됨
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN) # lock 해제, LOCK_UN 플래그로 OS에 lock 해제 요청
    """
       duckDB 자체의 lock을 없애는 함수가 아니라 DuckDB가 lock 에러를 발생하기 전에 애플리케이션 레벨에서 쓰기 프로세스를 하나로 직렬화
       Airflow pool과 같이 사용
       - OS lock은 실행된 프로세스 안에서 동시 쓰기 에러가 발생하지 않도록 보장
       - task가 동시에 실행 큐에 올라가지 않게 하는 스케줄러에서 보장
       - pool을 사용해서 불필요한 task 점유를 줄임
       - pool을 사용해서 timeout 실패를 줄임(정상적인 대기를 Airflow 스케줄러에 맡기는 것)
       - 따라서 둘 다 사용하는 것이 가장 안전하고 효율적임
       - pool은 동시에 실행하지 않게 하는 운영 정책, OS lock은 그래도 동시에 들어오면 막는 안전 장치
    """


@contextmanager # with 구문으로 블록 안의 작업이 하나의 트랜잭션으로 묶임
def open_mart_transaction(connection): # OS lock 획득 후 connection 연결 후, 해당 connection을 인자로 받음
    """
    DuckDB에 쓰기 작업을 할 때 transaction을 관리하는 Context manager
    - mart 쓰기 작업과 ops.mart_loaded_silver_file 테이블에 기록하는 작업이 하나의 transaction으로 묶이도록 보장
    """
    connection.execute("BEGIN TRANSACTION") # 트랜잭션 시작, 이 줄 이후 COMMIT 전까지 작업이 하나의 트랜잭션으로 묶임
    try:
        yield connection # with 블록 내부로 제어권을 넘김
        connection.execute("COMMIT") # 블록 코드가 성공적으로 실행되면 트랜잭션 커밋, 변경 사항이 DB에 영구적으로 반영됨
    except Exception:
        connection.execute("ROLLBACK") # 블록 코드가 예외를 발생시키면 트랜잭션 롤백
        raise
    """
        데이터 쓰기 작업을 하나의 트랜잭션으로 묶지 않고 작은 단위의 배치로 나누어서 처리
    """


# mart 적재 완료 로그를 기록할 공용 ops 테이블을 준비한다.
def ensure_mart_loaded_silver_file_table():
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError("Airflow 실행 환경에 duckdb 패키지가 없습니다.") from exc
    mart_path = LOCAL_S3_ROOT / "mart" / "stock_signal.duckdb"
    mart_path.parent.mkdir(parents=True, exist_ok=True)
    with open_mart_write_lock():
        with duckdb.connect(str(mart_path)) as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS ops")
            connection.execute("CREATE TABLE IF NOT EXISTS ops.mart_loaded_silver_file (source VARCHAR, silver_path VARCHAR, loaded_at TIMESTAMP, dag_id VARCHAR, run_id VARCHAR)")
    return {"mart_path": str(mart_path)}


# mart 적재가 끝난 silver 파일을 같은 transaction 안에서 기록한다.
def mark_silver_file_mart_loaded(connection, source, silver_path, loaded_at, dag_id, run_id):
    silver_path_value = str(silver_path)
    loaded_at_value = loaded_at.isoformat() if hasattr(loaded_at, "isoformat") else str(loaded_at)
    connection.execute(
        "INSERT INTO ops.mart_loaded_silver_file SELECT ?, ?, CAST(? AS TIMESTAMP), ?, ? WHERE NOT EXISTS (SELECT 1 FROM ops.mart_loaded_silver_file WHERE source = ? AND silver_path = ?)",
        [source, silver_path_value, loaded_at_value, dag_id, run_id, source, silver_path_value],
    )


# 지정 기간의 manifest에서 아직 mart에 적재되지 않은 silver path를 찾는다.
def find_pending_manifest_paths(connection, source, start_created_date, end_created_date, max_file_count):
    import json
    from datetime import datetime, timedelta

    started_on = datetime.strptime(str(start_created_date), "%Y-%m-%d").date()
    ended_on = datetime.strptime(str(end_created_date), "%Y-%m-%d").date()
    if ended_on < started_on:
        raise ValueError("end_created_date must be greater than or equal to start_created_date")
    max_file_count_value = int(max_file_count)
    if max_file_count_value <= 0:
        return []
    loaded_paths = {row[0] for row in connection.execute("SELECT silver_path FROM ops.mart_loaded_silver_file WHERE source = ?", [source]).fetchall()}
    pending_paths = []
    manifest_root = LOCAL_S3_ROOT / "silver" / "_created_manifest" / f"source={source}"
    current_date = started_on
    while current_date <= ended_on and len(pending_paths) < max_file_count_value:
        manifest_paths = (manifest_root / f"created_date={current_date.isoformat()}").glob("collection_id=*/manifest.json")
        for manifest_path in sorted(manifest_paths):
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            for silver_path in manifest.get("silver_paths", []):
                if silver_path in loaded_paths:
                    continue
                pending_paths.append({"source": source, "collection_id": manifest["collection_id"], "manifest_path": str(manifest_path), "silver_path": silver_path})
                loaded_paths.add(silver_path)
                if len(pending_paths) >= max_file_count_value:
                    break
            if len(pending_paths) >= max_file_count_value:
                break
        current_date += timedelta(days=1)
    return pending_paths


def validate_mart_loaded_silver_files(connection, source, silver_paths):
    """
    mart에 적재된 silver 파일이 manifest의 silver_paths와 일치하는지 검증한다.
    """
    expected_paths = [str(silver_path) for silver_path in silver_paths]
    if not expected_paths:
        return {"source": source, "validated_count": 0}
    loaded_paths = {row[0] for row in connection.execute("SELECT silver_path FROM ops.mart_loaded_silver_file WHERE source = ?", [source]).fetchall()}
    missing_paths = [silver_path for silver_path in expected_paths if silver_path not in loaded_paths]
    if missing_paths:
        raise ValueError(f"missing mart loaded silver file records: source={source}, silver_paths={missing_paths}")
    return {"source": source, "validated_count": len(expected_paths)}


def summarize_pending_silver_backlog(connection, reference_time, lookback_minutes, sources=None):
    """
    지정 기간의 manifest와 mart 적재 로그를 비교해 source별 pending silver 파일 수를 요약한다.
    """
    import json
    import pendulum

    reference_at = pendulum.parse(str(reference_time))
    started_at = reference_at.subtract(minutes=int(lookback_minutes))
    source_values = [sources] if isinstance(sources, str) else list(sources or ["kis_stock_price", "mk_rss"])
    source_summaries = {}
    total_pending_count = 0
    for source in source_values:
        loaded_paths = {row[0] for row in connection.execute("SELECT silver_path FROM ops.mart_loaded_silver_file WHERE source = ?", [source]).fetchall()}
        pending_count = 0
        silver_file_count = 0
        manifest_count = 0
        oldest_pending_created_at = None
        manifest_root = LOCAL_S3_ROOT / "silver" / "_created_manifest" / f"source={source}"
        current_date = started_at.start_of("day")
        while current_date <= reference_at:
            for manifest_path in sorted((manifest_root / f"created_date={current_date.format('YYYY-MM-DD')}").glob("collection_id=*/manifest.json")):
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                created_at = pendulum.parse(str(manifest["created_at"]))
                if created_at < started_at or created_at > reference_at:
                    continue
                manifest_count += 1
                for silver_path in manifest.get("silver_paths", []):
                    silver_file_count += 1
                    if silver_path in loaded_paths:
                        continue
                    pending_count += 1
                    oldest_pending_created_at = created_at if oldest_pending_created_at is None or created_at < oldest_pending_created_at else oldest_pending_created_at
            current_date = current_date.add(days=1)
        pending_age_minutes = reference_at.diff(oldest_pending_created_at).in_minutes() if oldest_pending_created_at else 0
        total_pending_count += pending_count
        source_summaries[source] = {"manifest_count": manifest_count, "silver_file_count": silver_file_count, "pending_count": pending_count, "oldest_pending_created_at": oldest_pending_created_at.to_iso8601_string() if oldest_pending_created_at else None, "pending_age_minutes": pending_age_minutes, "has_stale_backlog": pending_count > 0 and pending_age_minutes >= int(lookback_minutes)}
    return {"checked_from": started_at.to_iso8601_string(), "checked_to": reference_at.to_iso8601_string(), "lookback_minutes": int(lookback_minutes), "total_pending_count": total_pending_count, "sources": source_summaries}
