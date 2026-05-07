import json
from pathlib import Path

import pendulum


LOCAL_S3_ROOT = Path("/opt/airflow/s3")


def build_quality_check_result(source, stage, check_name, status, message, details=None):
    """
    데이터 품질 결과를 표준 형식의 딕셔너리로 만드는 헬퍼 함수입니다.
    """
    return {
        "source": source, # 어떤 데이터 소스인지
        "stage": stage, # 데이터 처리 단계(ex. bronze, silver, mart)
        "check_name": check_name, # 수행한 품질 체크 이름
        "status": status, # 체크 결과
        "checked_at": pendulum.now("Asia/Seoul").to_iso8601_string(), # 체크 수행 시각
        "message": message, # 체크 결과에 대한 설명
        "details": details or {}, # 추가 상세 정보
    }


def write_quality_check_result(context, quality_check_result):
    """
    데이터 품질 체크 결과를 운영용 JSON 파일로 저장합니다.
    - 반환 값 : 저장된 JSON 파일 경로
    """
    # Airflow의 context에서 Dag, Task, Run 정보를 가져오기
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")
    dag_id = getattr(task_instance, "dag_id", getattr(context.get("dag"), "dag_id", None))
    task_id = getattr(task_instance, "task_id", context.get("task_id"))
    run_id = getattr(dag_run, "run_id", context.get("run_id"))
    check_name = quality_check_result["check_name"] # 체크 이름을 파일 이름에 포함시키기 위해 가져오기
    # 결과를 저장할 경로 생성 : /opt/airflow/s3/ops/quality_check_result/dag_id=xxx/run_id=xxx/task_id=xxx/check_name=xxx.json
    result_path = (
        LOCAL_S3_ROOT
        / "ops"
        / "quality_check_result"
        / f"dag_id={dag_id}"
        / f"run_id={run_id}"
        / f"task_id={task_id}"
        / f"check_name={check_name}.json"
    )
    # 결과를 JSON 파일로 저장하기 전에, 결과가 저장될 디렉토리가 존재하는지 확인하고, 없으면 생성
    result_path.parent.mkdir(parents=True, exist_ok=True)
    # JSON 파일로 저장할 데이터 구성 : Airflow 실행 정보 + 품질 체크 결과
    result_payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "task_id": task_id,
        **quality_check_result, # dict unpacking
    }
    # 결과를 JSON 파일로 저장하기
    with result_path.open("w", encoding="utf-8") as file:
        json.dump(result_payload, file, ensure_ascii=False, indent=2)
        
    return {"quality_check_result_path": str(result_path)} # 저장된 결과 파일 경로 반환


def ensure_quality_check_passed(quality_check_result):
    """
    데이터 품질 체크 결과가 pass인지 확인하고, 데이터 품질 실패면 Task에서 예외를 발생시킵니다.
    """
    # TODO: pass를 상수로 관리하기
    if quality_check_result.get("status") == "pass":
        return quality_check_result
    # 품질 체크 실패면 예외 발생 시키기, Airflow에서 실패한 Task로 기록되고, on_failure_callback이 있다면 콜백 함수가 실행됨
    raise ValueError(
        "데이터 품질 체크 실패: "
        f"source={quality_check_result.get('source')}, "
        f"stage={quality_check_result.get('stage')}, "
        f"check_name={quality_check_result.get('check_name')}, "
        f"message={quality_check_result.get('message')}"
    )


def summarize_quality_check_results(reference_time, lookback_minutes):
    """
    지정 기간의 품질 체크 JSON을 읽어 source, stage, status별 결과를 요약합니다.
    """
    reference_at = pendulum.parse(str(reference_time))
    started_at = reference_at.subtract(minutes=int(lookback_minutes))
    summary = {}
    failures = []
    result_root = LOCAL_S3_ROOT / "ops" / "quality_check_result"
    for result_path in sorted(result_root.rglob("*.json")) if result_root.exists() else []:
        try:
            result = json.loads(result_path.read_text(encoding="utf-8"))
            checked_at = pendulum.parse(str(result["checked_at"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            continue
        if checked_at < started_at or checked_at > reference_at:
            continue
        source_summary = summary.setdefault(result.get("source"), {})
        stage_summary = source_summary.setdefault(result.get("stage"), {})
        stage_summary[result.get("status")] = stage_summary.get(result.get("status"), 0) + 1
        if result.get("status") != "pass":
            failures.append({"source": result.get("source"), "stage": result.get("stage"), "check_name": result.get("check_name"), "message": result.get("message"), "checked_at": result.get("checked_at"), "path": str(result_path)})
    return {"checked_from": started_at.to_iso8601_string(), "checked_to": reference_at.to_iso8601_string(), "summary": summary, "failure_count": len(failures), "failures": failures}


def notify_hourly_quality_check_summary(reference_time=None, lookback_minutes=60):
    """
    최근 품질 체크 요약을 Google Chat으로 전송합니다.
    """
    from google_chat_alert_pipeline import _read_google_chat_webhook_url, send_google_chat_message

    reference_at = reference_time or pendulum.now("Asia/Seoul").to_iso8601_string()
    quality_summary = summarize_quality_check_results(reference_at, lookback_minutes)
    lines = [
        "[Pipeline quality hourly summary]",
        f"checked_from: {quality_summary['checked_from']}",
        f"checked_to: {quality_summary['checked_to']}",
        f"failure_count: {quality_summary['failure_count']}",
    ]
    for source, source_summary in sorted(quality_summary["summary"].items()):
        for stage, stage_summary in sorted(source_summary.items()):
            status_counts = ", ".join([f"{status}={count}" for status, count in sorted(stage_summary.items())])
            lines.append(f"{source}/{stage}: {status_counts}")
    for failure in quality_summary["failures"][:5]:
        lines.append(f"failure: {failure['source']}/{failure['stage']}/{failure['check_name']} - {failure['message']}")
    webhook_url = _read_google_chat_webhook_url()
    if not webhook_url:
        return {**quality_summary, "sent": False, "reason": "missing_google_chat_webhook_url"}
    return {**quality_summary, "sent": True, "send_result": send_google_chat_message(webhook_url, "\n".join(lines))}
