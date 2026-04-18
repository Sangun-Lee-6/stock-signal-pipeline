# 1단계 : 원천 데이터 이해하기(RSS)

## 1.1 RSS 원천 데이터 이해하기

- 데이터 1줄 표현 : 특정 시점에 수집한 특정 RSS 피드 안의 기사(item) 1건
- 데이터 레코드 하나의 Grain : `source_feed` 기준 RSS 기사 1건
  - `grain = source_feed x article_no`
- RSS 응답은 channel 아래 여러 개의 `item` 배열로 구성됨
  - 각 item은 기사 1건의 메타데이터와 요약 본문을 담고 있음
    - 예: `no`, `title`, `link`, `published_at`, `description` 등이 기사 1건에 대응됨
  - 단, 같은 기사가 여러 피드에 중복 노출될 수 있으므로 원천 grain과 canonical article grain은 다를 수 있음
    - 예: `12016466` 기사는 경제 피드와 증권 피드에 모두 등장함

---

## 1.2 컬럼 그룹핑해서 이해하기

- 비즈니스적 역할로 그룹핑
- 식별 컬럼(기사를 식별하는 값들)
  - `items[].all_fields.no` = 기사 번호
  - `items[].link` = 기사 URL
  - `items[].title` = 기사 제목
  - `items[].source_feed` = 피드 식별자
  - ✅ 데이터 중복 가능성 : 동일 기사가 여러 RSS 피드에 동시에 실릴 수 있으므로 `article_no` 또는 `link` 기준 canonicalization 검토 필요
- 시점 컬럼(데이터가 언제 발행/수집된 것인가)
  - `collected_at` = 실제 파이프라인 수집 시각
  - `channel.lastBuildDate` = 피드 마지막 생성 시각
  - `items[].pub_date` = RSS 원문 발행 시각 문자열
  - `items[].published_at` = 표준화된 발행 시각
  - ✅ 운영 기준 시점은 `collected_at`, 기사 기준 시점은 `published_at`으로 나눠서 봐야 함
- 기사 내용/이벤트 후보 컬럼
  - `items[].title` = 기사 제목
  - `items[].description` = 기사 요약 본문
  - `items[].categories` = 기사 카테고리
  - `items[].matched_keywords` = 키워드 매칭 결과
  - `items[].is_important_candidate` = 중요 기사 후보 여부
  - ✅ 이 데이터들은 시세 fact가 아니라 이벤트 후보 추출의 재료가 됨
  - 예: 반도체/수출/환율 관련 키워드가 붙은 기사 존재
- 소스/수집 메타 컬럼
  - `source`
  - `source_feed`
  - `source_feed_name`
  - `source_feed_label`
  - `source_feed_url`
  - `collection_id`
  - `response.status_code`
  - ✅ 분석보다 운영/재처리/모니터링용 값에 가까움
- 미디어/부가 컬럼
  - `items[].all_fields.content.attributes.medium`
  - `items[].all_fields.content.attributes.url`
  - `items[].raw_item_xml`
  - ✅ 기사 카드 UI나 디버깅에는 유용하지만, 이벤트 모델의 핵심 컬럼은 아님

---

## 1.3 모델링 포인트

1. 이 데이터는 상태 스냅샷이 아니라 기사 기반 이벤트 후보 데이터

- KIS는 특정 시점의 종목 상태 스냅샷이었지만 RSS는 기사 1건 단위의 텍스트 데이터이며, 가격에 영향을 줄 수 있는 사건 후보를 담고 있음
- 가격 축 : KIS
- 이벤트 축 : RSS, OpenDART
- 즉 RSS는 바로 가격 fact가 아니라 이벤트 source 로 봐야 함

1. RSS item 하나에는 기사 식별 정보, 발행 정보, 텍스트 정보, 파생 신호가 섞여 있음

- 예시
  - `no`, `link`, `title` → 기사 식별/대표 정보
  - `published_at` → 시점 정보
  - `description` → 이벤트 해석용 텍스트
  - `matched_keywords`, `is_important_candidate` → 파이프라인이 추가한 파생값
- 즉 하나의 JSON item 안에
  - 원천 기사 메타데이터
  - 파이프라인 파생 신호
  - feed 레벨 반복 속성
    이 섞여 있으므로 그대로 mart에 넣으면 안 됨

1. 기사와 이벤트는 동일하지 않음

- 기사 1건이 이벤트 1건일 수도 있지만, 항상 그렇지는 않음
- 예:
  - `[포토]`, `[표]` 같은 항목은 이벤트 분석 가치가 낮음
  - 어떤 기사는 여러 기업/산업/거시 키워드를 동시에 담을 수 있음
- 따라서 RSS raw → normalized event 로 한 번 더 해석/정규화하는 단계가 필요함

---

## 1.4 이 데이터에 대한 분석 메모

- 1행의 의미 : 특정 RSS 피드에서 수집한 기사(item) 1건
- 관측키 후보
  - `collection_id + source_feed + article_no`
- 비즈니스 키 후보
  - `article_no`
  - `link`
- 시계열 기준 컬럼
  - `published_at`
  - 운영 기준용으로는 `collected_at`
- 주요 측정/해석 대상 값
  - `title`
  - `description`
  - `categories`
  - `matched_keywords`
  - `is_important_candidate`

---

## JSON

```json
{
  "source": "mk_rss",
  "source_feed": "mk_stock",
  "source_feed_name": "Maeil Business Newspaper Stock",
  "source_feed_label": "stock",
  "source_feed_url": "https://www.mk.co.kr/rss/50200011/",
  "collected_at": "2026-04-15T07:00:01.384466+09:00",
  "collection_id": "20260415T070001_fd8de58f",

  "channel": {
    "title": "매일경제 : 증권",
    "link": "https://www.mk.co.kr/",
    "description": "매일경제 : 증권",
    "language": "ko",
    "copyright": "Copyright 2026 MK",
    "lastBuildDate": "Wed, 15 Apr 2026 07:00:00 +09:00"
  },

  "items": [
    {
      "source_feed": "mk_stock",
      "source_feed_name": "Maeil Business Newspaper Stock",
      "source_feed_url": "https://www.mk.co.kr/rss/50200011/",
      "title": "코스피 장중 6000 재돌파…반도체, 금융주가 강세 이끌었다",
      "link": "https://www.mk.co.kr/news/stock/12016691",
      "description": "코스피가 1개월여만에 6000선을 재돌파했다...",
      "author": "매일경제",
      "pub_date": "Tue, 14 Apr 2026 17:19:49 +09:00",
      "published_at": "2026-04-14T17:19:49",
      "categories": ["증권"],
      "matched_keywords": ["반도체"],
      "is_important_candidate": true,
      "all_fields": {
        "no": "12016691",
        "category": "증권",
        "content": {
          "attributes": {
            "medium": "image",
            "url": "https://..."
          }
        }
      }
    }
  ]
}
```

- 예시 기사
  - `코스피 장중 6000 재돌파…반도체, 금융주가 강세 이끌었다`
  - `반도체 수출 328억弗 기염...ICT 수출은 첫 400억弗 돌파`
  - `SP삼화, 반도체 패키징 핵심소재 EMC 상용화`
