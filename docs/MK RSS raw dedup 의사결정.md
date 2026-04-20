# MK RSS raw dedup 의사결정

## 문서 목적

이 문서는 `MK RSS` raw 수집 단계에서 dedup을 적용할지에 대한 문제 상황, 문제 원인, 해결 옵션, 결론을 한 문서로 정리한 의사결정 문서다.

판단 대상은 `raw bronze 단계에서 poll 간 동일 기사 item을 중복 저장할지, 저장하지 않을지`다.

---

## 1. 문제 상황

- `MK RSS`는 최신 `50개` 기사만 feed에 노출된다.
- 현재 수집 방식은 `10분 주기 polling`이다.
- 같은 기사가 여러 polling 구간에 반복 노출될 수 있다.
- 현재 raw bronze는 `collection_id` 단위로 RSS XML snapshot 전체를 저장한다.
- 이 구조에서는 같은 기사도 polling 시점이 다르면 raw에 반복 저장된다.
- 그래서 아래 두 요구가 동시에 존재한다.
  - 원천 snapshot을 그대로 보존하고 싶다.
  - 중복 저장 비용과 downstream dedup 부담은 줄이고 싶다.

즉, 이 문제는 단순 저장 최적화가 아니라 `bronze를 snapshot evidence layer로 볼지, 고유 기사 적재 layer로 볼지`에 대한 결정이다.

---

## 2. 문제 원인

### 2-1. source 자체가 휘발적이다

- `MK RSS`는 최신 `50개`만 유지되는 롤링 윈도우 구조다.
- 한번 놓친 기사는 이후 재조회로 복구하기 어렵다.
- 그래서 일반 API보다 `원문 snapshot 보존 가치`가 더 크다.

### 2-2. 현재 raw의 책임이 snapshot 보존에 가깝다

- 현재 적재 단위는 `고유 기사 집합`이 아니라 `특정 시점 feed 상태`다.
- raw에서 dedup을 넣으면 저장 최적화가 아니라 레이어 책임 자체가 바뀐다.

### 2-3. 안정적인 dedup key가 아직 충분히 검증되지 않았다

- `guid`, `link`, `title`, `pubDate`, 기사 번호(`no`) 중 무엇을 동일성 기준으로 쓸지 검증이 더 필요하다.
- key가 흔들리면 신규 기사 누락, 기존 기사 수정 이력 손실, 중복 판정 오류가 발생할 수 있다.

### 2-4. 비용 우려는 있으나 현재 수치는 크지 않다

- 현재 관측 기준 raw 파일 `1건 = 41,904 bytes`
- `10분 주기` 적재 시 `1년 약 2.05 GiB`
- S3 기준 `1년치 누적 데이터의 월 저장료 약 $0.051`

현재 시점에서는 저장 비용 자체보다 `누락 리스크`와 `원문 보존 가치`가 더 큰 판단 요소다.

---

## 3. 해결 옵션

### 옵션 A. raw bronze에서 dedup을 적용하지 않는다

의미

- 매 polling 시점의 RSS feed snapshot을 그대로 raw에 저장한다.

장점

- 원천 feed를 본 형태 그대로 보존할 수 있다.
- 특정 시점의 feed 상태를 snapshot 기준으로 복원할 수 있다.
- dedup key 설계 없이 구현이 단순하다.
- 잘못된 dedup 판정으로 신규 기사를 놓칠 위험이 없다.
- 장애 분석, 재처리, 원문 기준 디버깅에 유리하다.

단점

- 같은 기사 XML이 반복 저장된다.
- downstream에서 dedup을 별도로 처리해야 한다.
- raw volume이 실제 신규 기사 수보다 더 빠르게 증가한다.

### 옵션 B. raw bronze에서 dedup을 적용한다

의미

- 이전에 저장한 기사와 동일하다고 판단되는 item은 raw에 다시 저장하지 않는다.

장점

- 중복 저장을 줄여 storage cost를 낮출 수 있다.
- downstream 중복 제거 부담이 줄어든다.
- 신규 기사 수와 raw 적재량의 대응 관계가 더 명확해진다.

단점

- dedup key와 비교 기준을 먼저 확정해야 한다.
- key 오류 시 신규 기사 누락 위험이 있다.
- 특정 시점의 원천 snapshot을 그대로 복원하기 어렵다.
- source 수정이나 재노출 이력을 놓칠 수 있다.
- 휘발성 source 특성상 잘못 누락된 데이터를 복구하기 어렵다.

### 옵션 C. raw snapshot은 유지하고, downstream에서 dedup을 분리한다

의미

- raw bronze는 지금처럼 snapshot을 저장한다.
- silver 또는 후속 레이어에서 `기사 단위 dedup` 데이터를 별도로 만든다.
- 저장 비용이 문제로 커지면 raw dedup보다 먼저 `raw TTL`을 조정한다.

장점

- raw evidence 보존과 dedup된 소비 레이어를 동시에 가져갈 수 있다.
- 누락 리스크를 raw 단계로 끌어올리지 않는다.
- 비용 제어를 dedup 로직이 아니라 보존 정책으로 먼저 대응할 수 있다.

단점

- 레이어가 하나 더 늘어나고 운영 정책이 추가된다.
- raw와 silver의 역할을 명확히 관리해야 한다.

---

## 4. 옵션 비교

| 구분                 | 옵션 A. raw dedup 안 함 | 옵션 B. raw dedup 함 | 옵션 C. raw 유지 + downstream dedup |
| -------------------- | ----------------------- | -------------------- | ----------------------------------- |
| raw 역할             | snapshot evidence       | 고유 기사 적재       | snapshot evidence                   |
| 저장 비용            | 높음                    | 낮아짐               | raw는 유지, 장기 비용은 별도 제어   |
| 구현 난이도          | 낮음                    | 높음                 | 중간                                |
| 누락 리스크          | 낮음                    | 높음                 | 낮음                                |
| snapshot 복원        | 쉬움                    | 어려움               | 쉬움                                |
| downstream 사용성    | dedup 추가 필요         | 바로 활용 쉬움       | dedup 레이어 활용 가능              |
| 현재 프로젝트 적합도 | 높음                    | 낮음                 | 가장 현실적                         |

---

## 5. 현재 프로젝트 적용 상태

현재 구현은 `옵션 C`와 일치한다.

- `airflow/plugins/mk_rss_pipeline.py`에서 raw payload는 `collection_id`별 bronze 경로에 그대로 저장된다.
- 같은 파일의 `write_mk_rss_bronze_to_silver`는 RSS snapshot을 기사 1건 단위로 파싱하고, `silver_news_event/source_feed=.../published_date=.../article_id=.../data.parquet` 경로에 저장한다.
- 이 구조에서는 같은 기사가 여러 polling 구간에 반복 노출돼도 `silver`는 `article_id` 기준 canonical article 형태로 정리된다.
- `write_mk_rss_silver_to_mart`도 `mk_rss:{article_id}` 기준 `NOT EXISTS`로 중복 적재를 막는다.
- `docs/data-modeling/silver 스키마 정의.md`에도 `silver_news_event`의 유일성 기준을 `article_id`로 두고, 반복 노출된 동일 기사를 `silver`에서 정리한다고 명시돼 있다.

즉, 현재 프로젝트는 `raw snapshot 보존 + silver article 기준 정리 + mart 중복 방지` 구조다.

다만 현재 `silver`의 dedup은 별도 dedup 배치라기보다 `article_id` 기준 canonical 경로에 저장하는 방식에 가깝다.

---

## 6. 결론

현재 프로젝트 기준으로는 `옵션 C`가 가장 적절하다.

- raw bronze는 지금처럼 `feed snapshot raw`를 보존한다.
- dedup은 raw가 아니라 silver 또는 후속 레이어에서 `기사 단위`로 처리한다.
- 비용 제어가 필요하면 raw dedup보다 먼저 `raw 보존 기간 축소`를 검토한다.

이 결론의 이유는 아래와 같다.

- `MK RSS`는 휘발성이 커서 원문을 놓치면 복구가 어렵다.
- 현재 raw는 snapshot 보존 레이어로 설계되어 있다.
- 안정적인 dedup key는 추가 검증이 필요하다.
- 현재 관측 비용은 구조 변경을 정당화할 만큼 크지 않다.
- 실제 구현도 이미 `bronze raw 보존`과 `silver/article_id 기준 정리` 방향으로 되어 있다.

즉, 현재 권장안은 `raw는 보존`, `dedup은 downstream`, `비용 제어는 TTL 우선`이다.

---
