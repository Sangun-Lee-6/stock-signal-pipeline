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
