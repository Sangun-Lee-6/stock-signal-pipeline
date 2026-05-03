from pathlib import Path


LOCAL_S3_ROOT = Path("/opt/airflow/s3")


# mart 적재 완료 로그를 기록할 공용 ops 테이블을 준비한다.
def ensure_mart_loaded_silver_file_table():
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError("Airflow 실행 환경에 duckdb 패키지가 없습니다.") from exc
    mart_path = LOCAL_S3_ROOT / "mart" / "stock_signal.duckdb"
    mart_path.parent.mkdir(parents=True, exist_ok=True)
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
