import json
import logging
import os
from urllib import error, request


def _read_google_chat_webhook_url():
    """
    환경 변수에서 GOOGLE_CHAT_WEBHOOK_URL을 읽어옵니다. 
    - 만약 환경 변수에 설정되어 있지 않으면 None을 반환합니다.
    """
    webhook_url = os.environ.get("GOOGLE_CHAT_WEBHOOK_URL") # 환경 변수에서 GOOGLE_CHAT_WEBHOOK_URL을 읽어옵니다. 값이 없으면 None이 됩니다.
    return webhook_url or None # None이거나 빈 문자열("")인 경우 None을 반환합니다. 그렇지 않으면 webhook_url을 반환합니다. 


def build_google_chat_failure_message(context): # context : Airflow에서 callback 함수에 넘겨주는 실행 정보 묶음(Dag 정보, Task 정보, 실행 시각, 예외 정보 등)
    """
    Airflow 실패 context에서 Google Chat 메시지 본문을 만듭니다.
    """
    task_instance = context.get("task_instance") # task 실행정보
    dag_run = context.get("dag_run") # Dag 실행 정보
    exception = context.get("exception") # 예외 정보
    dag_id = getattr(task_instance, "dag_id", getattr(context.get("dag"), "dag_id", None)) # Dag ID 가져오기 : 우선순위 1. task_instance에서 dag_id -> 우선순위 2. context의 dag에서 dag_id -> 우선순위 3. None
    exception_text = f"{type(exception).__name__}: {exception}" if exception else None # 예외 정보가 있다면 예외의 타입과 메세지를 보기 좋게 문자열로 구성
    # 메세지 본문 생성
    return "\n".join([
        "[Airflow task failed]",
        f"dag_id: {dag_id}", # 어떤 Dag에서 실패했는지
        f"task_id: {getattr(task_instance, 'task_id', None)}", # 어떤 Task에서 실패했는지
        f"run_id: {getattr(dag_run, 'run_id', context.get('run_id'))}", # 어떤 실행인지
        f"logical_date: {context.get('logical_date')}", # Airflow 기준 데이터 처리 날짜
        f"try_number: {getattr(task_instance, 'try_number', None)}", # 몇 번째 재시도 횟수인지 
        f"exception: {exception_text}", # 예외 정보
        f"log_url: {getattr(task_instance, 'log_url', None)}", # Ariflow 로그 링크
    ])


def send_google_chat_message(webhook_url, message_text):
    """
    Google Chat webhook으로 메시지를 전송합니다.
    """
    payload = json.dumps({"text": message_text}).encode("utf-8") # 파이썬 dict를 JSON 문자열로 변환하고, HTTP 요청을 보낼 수 있도록 utf-8 바이트로 인코딩
    # HTTP Post 요청 객체 생성
    chat_request = request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with request.urlopen(chat_request, timeout=10) as response: # HTTP 요청 보내고 응답 받기, 성공하면 HTTP 응답 코드 반환
            return {"status_code": response.status}
    except error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace") # 에러 상황 : 응답은 있는데 실패 상태 코드
        raise RuntimeError(f"Google Chat 알림 전송 실패: status={exc.code}, body={error_body}") from exc
    except error.URLError as exc: # 에러 상황 : 네트워크 문제 등으로 인해 요청 자체가 실패한 경우
        raise RuntimeError(f"Google Chat 알림 전송 실패: reason={exc.reason}") from exc


def notify_google_chat_on_failure(context):
    """
    Airflow task 실패 시 Google Chat으로 알림을 전송합니다.
    """
    logger = logging.getLogger(__name__) # 로깅 모듈에서 로거 객체 가져오기
    webhook_url = _read_google_chat_webhook_url() # 웹훅 URL 가져오기
    # 웹훅 URL이 없다면 알림 메세지를 보내지 않고 로그 남기기
    if not webhook_url:
        logger.warning("GOOGLE_CHAT_WEBHOOK_URL이 비어 있어 Google Chat 알림을 건너뜁니다.")
        return
    # 웹훅 URL이 있다면 알림 메세지 만들고 보내기, 실패하면 예외 로그 남기기
    try:
        message_text = build_google_chat_failure_message(context)
        send_google_chat_message(webhook_url, message_text)
    except Exception:
        logger.exception("Google Chat 실패 알림 전송 중 오류가 발생했습니다.")
