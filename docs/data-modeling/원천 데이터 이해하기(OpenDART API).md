# 1단계 : 원천 데이터 이해하기(OpenDART)

## 1.1 OpenDART 원천 데이터 이해하기

- 데이터 1줄 표현 : 특정 조회 조건(기간, 페이지, 정렬 기준)으로 조회한 공시 목록 중 **공시 1건**
- 데이터 레코드 하나의 Grain : `rcept_no` 기준 공시 1건
  ```
  grain = rcept_no
  ```

  - 이 응답은 `response.body.list[]` 배열 안에 공시 목록이 여러 건 들어 있음
  - 각 원소는 개별 공시 1건의 메타데이터를 담고 있음

---

## 1.2 컬럼 그룹핑해서 이해하기

- 비즈니스적 역할로 그룹핑
- 식별 컬럼(공시를 식별하는 값들)
  - `response.body.list[].rcept_no` = 공시 접수 번호
  - `response.body.list[].corp_code` = DART 기업 고유 코드
  - `response.body.list[].corp_name` = 기업명
  - `response.body.list[].stock_code` = 상장 종목 코드
  - ✅ 데이터 중복 주의
    - 같은 기업이 하루에 여러 공시를 낼 수 있으므로 `corp_code`나 `stock_code`만으로는 1행을 식별할 수 없음
- 시점 컬럼(데이터가 언제 발생/접수된 것인가)
  - `collected_at` = 실제 파이프라인 수집 시각
  - `request.params.bgn_de`, `end_de` = 조회 범위
  - `response.body.list[].rcept_dt` = 공시 접수 일자
  - ✅ 운영 기준 시점은 `collected_at`
  - ✅ 공시 비즈니스 시점은 `rcept_dt`
- 공시 내용/이벤트 후보 컬럼
  - `response.body.list[].report_nm` = 공시명
  - `response.body.list[].flr_nm` = 제출인
  - `response.body.list[].rm` = 비고
  - `response.body.list[].corp_cls` = 법인 구분
  - 즉, RSS의 제목/요약처럼 이벤트 후보를 알려주지만, 세부 내용은 아직 없음
  - 예:
    - `주요사항보고서(유상증자결정)`
    - `전환청구권행사`
    - `감사보고서 (2025.12)`
    - `최대주주변경`
    - `주식병합결정`
- 소스/수집 메타 컬럼
  - `source`
  - `endpoint`
  - `collection_id`
  - `request.params.page_no`
  - `request.params.page_count`
  - `response.body.page_no`
  - `response.body.page_count`
  - `response.body.total_count`
  - `response.body.total_page`
  - `response.body.status`
  - `response.body.message`
  - ✅ 분석보다 운영/재처리/페이지네이션 관리용 값에 가까움

---

## 1.3 모델링 포인트

1. 이 데이터는 공시 원문이 아니라 **공시 목록 메타데이터**

- KIS는 시세 스냅샷
- RSS는 기사 item
- OpenDART `list.json`은 공시 상세 본문이 아닌 목록
- 즉 이 데이터는 “이벤트 그 자체”의 원문보다는 **이벤트 인덱스 목록**에 가까움
- 실무적으로 보면
  - 가격 축 : KIS
  - 기사 이벤트 축 : RSS
  - 공시 이벤트 축 : OpenDART list + 이후 상세 공시 본문

1. OpenDART 목록 1행에는 기업 식별 정보와 공시 식별 정보, 공시 유형 정보가 섞여 있음

- 예시
  - `corp_code`, `corp_name`, `stock_code` → 기업 식별 정보
  - `rcept_no`, `rcept_dt` → 공시 식별/시점 정보
  - `report_nm`, `flr_nm`, `rm` → 공시 타입/설명 정보
- 즉 하나의 레코드 안에
  - 기업 기준 정보
  - 공시 기준 정보
  - 제출자/비고 메타
    가 함께 있음
- 그대로 mart에 넣기보다, 적어도 공시 fact 후보 와 기업 기준 정보 를 분리하기

1. `report_nm`이 사실상 핵심 이벤트 신호

- 예:
  - `주요사항보고서(유상증자결정)` → 자본조달 이벤트
  - `전환청구권행사` → 희석 가능성/자본구조 이벤트
  - `최대주주변경` → 지배구조 이벤트
  - `주식병합결정` → 주식 구조 이벤트
- 상세 공시를 보기 전에 `report_nm`만으로 1차 이벤트 분류는 가능

---

## 1.4 이 데이터에 대한 분석 메모

- 1행의 의미 : OpenDART 공시 목록에서 조회된 공시 1건
- 관측키 후보
  - `rcept_no`
- 비즈니스 키 후보
  - `rcept_no`
- 기업 연결 키 후보
  - `corp_code`
  - `stock_code`
- 시계열 기준 컬럼
  - `rcept_dt`
  - 운영 기준용으로는 `collected_at`
- 주요 측정/해석 대상 값
  - `report_nm`
  - `corp_name`
  - `corp_cls`
  - `flr_nm`
  - `rm`

---

## JSON

```json
{
  "source": "opendart",
  "endpoint": "https://opendart.fss.or.kr/api/list.json",
  "collected_at": "2026-04-15T06:18:02.072433+09:00",
  "collection_id": "20260415T061802_52336a9a",

  "request": {
    "params": {
      "bgn_de": "20260414",
      "end_de": "20260415",
      "last_reprt_at": "Y",
      "sort": "date",
      "sort_mth": "desc",
      "page_no": "1",
      "page_count": "100"
    }
  },

  "response": {
    "status_code": 200,
    "body": {
      "status": "000",
      "message": "정상",
      "page_no": 1,
      "page_count": 100,
      "total_count": 2761,
      "total_page": 28,
      "list": [
        {
          "corp_code": "01118281",
          "corp_name": "피플바이오",
          "stock_code": "304840",
          "corp_cls": "K",
          "report_nm": "주요사항보고서(유상증자결정)",
          "rcept_no": "20260414002751",
          "flr_nm": "피플바이오",
          "rcept_dt": "20260415",
          "rm": ""
        }
      ]
    }
  }
}
```

- 예시 공시
  - `주요사항보고서(유상증자결정)` → 자본조달 이벤트 후보
  - `전환청구권행사` → 전환/희석 이벤트 후보
  - `최대주주변경` → 지배구조 이벤트 후보
  - `주식병합결정` → 주식 구조 변경 이벤트 후보
