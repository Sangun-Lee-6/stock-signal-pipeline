# 1단계 : 원천 데이터 이해하기(KIS Open API)

## 1.1 KIS 원천 데이터 이해하기

- 데이터 1줄 표현 : 특정 시점에 조회한 특정 종목(LX세미콘)의 현재 시세 스냅샷 1건
- 데이터 레코드 하나의 Grain : `collected_at` 시점의 `stock_code` 기준 시세 스냅샷 1건

  ```
  grain = collected_at x stock_code
  ```

  - 응답은 단일 종목 조회 결과이므로, `stock_code`(가격, 거래량, PER/PBR)
  - 이 값들은 종가 확정값이 아니라 조회 시점의 값이므로 `date x stock_code`가 아니라 `timestamp x stock_code`가 더 정확함

---

## 1.2 컬럼 그룹핑해서 이해하기

- 비즈니스적 역할로 그룹핑
- 식별 컬럼(종목을 식별하는 값들)
  - `stock.stock_code` = `108320`
  - `stock.stock_name` = `LX세미콘`
  - `stock.market_division_code` = `J`
  - `response.body.output.stck_shrn_iscd` = `108320`
  - ✅ 데이터 중복 : 종목 코드가 `stock_code`, `stck_shrn_iscd`에도 있으므로 정규화 단계에서 표준화 필요
- 시점 컬럼(데이터가 언제 관측된 값인가)
  - `collected_at` = 실제 파이프라인 수집 시각
  - 응답 헤더 `Date`
  - 일부 고/저점 날짜 컬럼
    - `d250_hgpr_date`
    - `d250_lwpr_date`
    - `dryy_hgpr_date`
    - `dryy_lwpr_date`
    - `w52_hgpr_date`
    - `w52_lwpr_date`
  - ✅ 기준 시점은 collected_at으로 설정해서 운영 기준 시점 통일
- 가격/거래 지표
  - `stck_prpr` 현재가
  - `stck_oprc` 시가
  - `stck_hgpr` 고가
  - `stck_lwpr` 저가
  - `stck_sdpr` 기준가/전일 종가 계열 해석 후보
  - `prdy_vrss` 전일 대비
  - `prdy_ctrt` 전일 대비율
  - `acml_vol` 누적 거래량
  - `acml_tr_pbmn` 누적 거래대금
  - `wghn_avrg_stck_prc` 가중평균 주가
  - ✅ 이 데이터들은 fact로 갈 확률이 높음
- 밸류에이션/기초 재무 성격 컬럼
  - 가격 자체보다는 해석용 보조 정보
  - `per`
  - `pbr`
  - `eps`
  - `bps`
  - `hts_avls` 시가총액 계열 해석 후보
  - `lstn_stcn` 상장주식수
  - ✅ 실시간 가격과 동일한 성격의 이벤트 데이터는 아니므로, 모델링할 때 fact, dim 분리할 수 있음
- 상태/시장 속성 컬럼
  - 분석보다 운영/필터링에 가까운 값
  - `rprs_mrkt_kor_name` = KOSPI
  - `bstp_kor_isnm` = 전기·전자
  - `temp_stop_yn`
  - `crdt_able_yn`
  - `ssts_yn`
  - `mrkt_warn_cls_code`
  - `short_over_yn`
  - `sltr_yn`
  - `mang_issu_cls_code`

---

## 1.3 모델링 포인트

1. 이 데이터는 이벤트가 아니라 상태 스냅샷

- 최종 목표는 주가와 주요 이벤트를 같이 보는 것
- 지금 데이터는 주가에 대한 데이터고, 이벤트에 대한 데이터(RSS, OpenDART)가 필요
- 가격 축 : KIS
- 이벤트 축 : RSS, OpenDART

1. KIS 응답 하나에는 종목 기준 정보와 시계열 관측값이 섞여 있음

- 예시
  - `stock_name`, `market_division_code`, `bstp_kor_isnm` → 비교적 기준정보에 가까움
  - `stck_prpr`, `acml_vol`, `prdy_ctrt` → 시점별 측정값
  - `per`, `pbr`, `eps`, `bps` → 준정적/배치 업데이트성 값
- 하나의 JSON 안에 서로 다른 수명 주기의 데이터가 섞여 있으므로, 그대로 mart에 넣으면 안됨

1. 숫자 타입은 전부 문자열

- 예시 : `"stck_prpr": "52500"`
- 따라서 Silver 표준화에서 데이터 타입을 캐스팅해줘야함
- 초반에 타입 정리와 컬럼명을 통일해야 이후 작업이 편해짐

---

## 1.4 이 데이터에 대한 분석 메모

- 1행의 의미 : 특정 시점에 특정 종목에 대해 KIS 응답 스냅샷
- 관측키 후보
  - `collection_id + stock_code`
  - 또는 `collected_at + stock_code`
- 비즈니스 키 후보
  - `stock_code`
- 시계열 기준 컬럼
  - `collected_at`
- 주요 측정값
  - `current_price, open_price, high_price, low_price,
volume_accumulated, trade_amount_accumulated,
change_value, change_rate,
per, pbr, eps, bps`

---

1. 이건 JSON 하나인데, 1행이 아니라 하나의 데이터 파일의 Grain인가요?
2. Grain은 데이터를 한마디로 표현한 것인가요? 추상화 수준을 높게 잡거나 낮게 잡으면 Grain이 달라지나요? 뭐가 좋은 Grain인가요?
3. Grain 어원이 뭔가요?
4. 만약 이 데이터에 PER값 하나만 있었다면 grain = collected_at x stock_code x per 인가요?
5. grain을 정의할 때 들어가는 값들은 변하는 값이 아니라 변하지 않는 값?으로 해야하나요?

## 궁금했던 점 정리

❓ 1. 이 데이터는 하나의 JSON인데, Grain 단위는 1행이라고 표현해야 하는가?

- 관측 단위의 Grain을 정해야 함
  - 파일의 Grain : 이 파일이 무엇을 담고 있는가
  - 레코드 Grain : 이 파일 안의 개별 데이터 단위가 무엇을 담고 있는가
- 현재 데이터 JSON는 파일 안의 핵심 비즈니스 데이터가 종목 1건에 대한 스냅샷 1건이므로, 논리적 단위는 1개
- 만약 JSON 응답이 다음과 같이 배열로 왔다면, 여기선 논리적 단위가 여러 개이므로, Grain은 각 items의 원소 1건을 기준으로 잡음
  ```json
  {
    "items": [
      { "stock_code": "005930", ... },
      { "stock_code": "000660", ... }
    ]
  }
  ```

❓ 2. Grain은 데이터를 한마디로 설명한 것인가?

- Grain은 이 테이블의 1행이 정확히 무엇을 의미하는지 정의하는 것
  - `date x stock_code` = 종목별 일별 1행
  - `collected_at x stock_code` = 종목별 수집시각별 1행
  - `news_id` = 뉴스 1건 1행
  - `disclosure_id` = 공시 1건 1행
- “의미적 PK”라고 이해했음
  - Grain은 집계, 조인, 중복 판정의 기준점

❓ 3. 어떤게 좋은 Grain인가?

- 추상화 수준을 바꾸면 Grain이 달라짐
- 예시 : 주가 데이터의 추상화 수준에 따른 Grain
  - `stock_code` → 너무 추상적이다. 언제의 가격인지 모름
  - `date x stock_code` → 일별 종가 수준
  - `minute x stock_code` → 분봉 수준
  - `collected_at x stock_code` → API 수집 스냅샷 수준
- 좋은 Grain의 조건 : 무조건 세밀한 수준이 좋은 Grain이 아님
  - 1행의 의미가 명확
  - 비즈니스 요구사항을 충족
  - 원천 데이터에서 구분 가능한 단위

❓ 4. Grain의 어원?

- grain : 곡물 알갱이 하나
- 즉, 더 이상 쪼개기 전에 다루는 최소 단위
- 데이터 모델링에서는 테이블이 표현하는 가장 작은 비즈니스 단위

❓ 5. 만약 이 데이터에 측정값이 PER값 하나만 있다면 `grain = collected_at x stock_code x per` 인가?

- NO, PER는 Grain을 구성하는 값이 아니라 Grain 위에서 측정된 속성값
- Grain을 구성하는 값은 행을 구분하는 기준이고, PER는 이 구분에 대한 측정값일 뿐

❓6. Grain을 정의할 때는 항상 변하지 않는 값들로 설정해야 하는가?

- Grain을 정할 때 들어가는 값은 불변이 기준이 아니라 행일 유일하게 식별하는 기준이어야 함
- 예시
  - collected_at은 변하지만 시간축이 있어야 레코드를 구분할 수 있으므로 grain에 포함
  - per는 변하지만 기준이 아니라 측정 결과이므로 grian으로 부적절

---

## JSON

```json
{
  "source": "kis_open_api", // 이 데이터가 어떤 원천 시스템에서 왔는지
  "endpoint": ".../inquire-price", // 호출한 API 엔드포인트
  "collected_at": "2026-04-15T18:00:01...", // 데이터를 실제로 수집한 시각
  "collection_id": "20260415T180001_...", // 이번 수집 실행을 식별하는 고유 ID

  "stock": {
    "stock_code": "108320", // 종목 코드
    "stock_name": "LX세미콘", // 종목명
    "market_division_code": "J" // 시장 구분 코드 (예: KOSPI/KOSDAQ 구분용)
  },

  "authentication": {
    "token_endpoint": ".../oauth2/tokenP", // 액세스 토큰 발급 API 주소
    "response": {
      "body": {
        "token_type": "Bearer", // 토큰 인증 방식
        "expires_in": 86400, // 토큰 유효 시간(초)
        "access_token_token_expired": "..." // 토큰 만료 시각
      }
    }
  },

  "request": {
    "headers": {
      "tr_id": "FHKST01010100", // KIS 거래 ID, 어떤 API 기능을 호출했는지 나타내는 코드
      "custtype": "P" // 고객 유형 코드 (보통 개인/법인 구분)
    },
    "params": {
      "fid_cond_mrkt_div_code": "J", // 조회 대상 시장 구분 코드
      "fid_input_iscd": "108320" // 조회한 종목 코드
    }
  },

  "response": {
    "status_code": 200, // HTTP 응답 상태 코드
    "body": {
      "output": {
        "iscd_stat_cls_code": "55", // 종목 상태 구분 코드
        "marg_rate": "30.00", // 증거금 비율
        "rprs_mrkt_kor_name": "KOSPI", // 대표 시장명
        "bstp_kor_isnm": "전기·전자", // 업종명
        "temp_stop_yn": "N", // 거래정지 여부
        "oprc_rang_cont_yn": "N", // 시가 범위 연속 여부
        "clpr_rang_cont_yn": "N", // 종가 범위 연속 여부
        "crdt_able_yn": "Y", // 신용거래 가능 여부
        "grmn_rate_cls_code": "40", // 보증금 비율 구분 코드
        "elw_pblc_yn": "N", // ELW 발행 여부

        "stck_prpr": "52500", // 현재가
        "prdy_vrss": "-300", // 전일 대비 가격 차이
        "prdy_vrss_sign": "5", // 전일 대비 부호 코드(상승/하락/보합)
        "prdy_ctrt": "-0.57", // 전일 대비 등락률(%)

        "acml_tr_pbmn": "4383382400", // 누적 거래대금
        "acml_vol": "83503", // 누적 거래량
        "prdy_vrss_vol_rate": "116.02", // 전일 대비 거래량 비율(%)

        "stck_oprc": "53100", // 시가
        "stck_hgpr": "53300", // 고가
        "stck_lwpr": "52100", // 저가
        "stck_mxpr": "68600", // 상한가
        "stck_llam": "37000", // 하한가
        "stck_sdpr": "52800", // 기준가
        "wghn_avrg_stck_prc": "52493.71", // 가중평균주가

        "hts_frgn_ehrt": "27.88", // 외국인 보유 비율(%)
        "frgn_ntby_qty": "0", // 외국인 순매수 수량
        "pgtr_ntby_qty": "-186", // 프로그램매매 순매수 수량

        "pvt_scnd_dmrs_prc": "53533", // 2차 저항선 가격(피벗 기준)
        "pvt_frst_dmrs_prc": "53166", // 1차 저항선 가격(피벗 기준)
        "pvt_pont_val": "52533", // 피벗 포인트 값
        "pvt_frst_dmsp_prc": "52166", // 1차 지지선 가격(피벗 기준)
        "pvt_scnd_dmsp_prc": "51533", // 2차 지지선 가격(피벗 기준)
        "dmrs_val": "53350", // 저항값
        "dmsp_val": "52350", // 지지값

        "cpfn": "81", // 자본금(축약 값)
        "rstc_wdth_prc": "15800", // 가격제한폭
        "stck_fcam": "500", // 액면가
        "stck_sspr": "38010", // 대용가
        "aspr_unit": "100", // 호가 단위
        "hts_deal_qty_unit_val": "1", // 매매 수량 단위

        "lstn_stcn": "16264300", // 상장 주식 수
        "hts_avls": "8539", // 시가총액
        "per": "10.33", // PER
        "pbr": "0.76", // PBR
        "stac_month": "12", // 결산월
        "vol_tnrt": "0.51", // 회전율
        "eps": "5081.00", // EPS
        "bps": "69207.00", // BPS

        "d250_hgpr": "68000", // 250일 최고가
        "d250_hgpr_date": "20250701", // 250일 최고가 발생일
        "d250_hgpr_vrss_prpr_rate": "-22.79", // 현재가와 250일 최고가의 차이율(%)

        "d250_lwpr": "46150", // 250일 최저가
        "d250_lwpr_date": "20250409", // 250일 최저가 발생일
        "d250_lwpr_vrss_prpr_rate": "13.76", // 현재가와 250일 최저가의 차이율(%)

        "stck_dryy_hgpr": "65600", // 연중 최고가
        "dryy_hgpr_vrss_prpr_rate": "-19.97", // 현재가와 연중 최고가 차이율(%)
        "dryy_hgpr_date": "20260225", // 연중 최고가 발생일

        "stck_dryy_lwpr": "47000", // 연중 최저가
        "dryy_lwpr_vrss_prpr_rate": "11.70", // 현재가와 연중 최저가 차이율(%)
        "dryy_lwpr_date": "20260304", // 연중 최저가 발생일

        "w52_hgpr": "68000", // 52주 최고가
        "w52_hgpr_vrss_prpr_ctrt": "-22.79", // 현재가와 52주 최고가 차이율(%)
        "w52_hgpr_date": "20250701", // 52주 최고가 발생일

        "w52_lwpr": "47000", // 52주 최저가
        "w52_lwpr_vrss_prpr_ctrt": "11.70", // 현재가와 52주 최저가 차이율(%)
        "w52_lwpr_date": "20260304", // 52주 최저가 발생일

        "whol_loan_rmnd_rate": "2.12", // 대차잔고 비율
        "ssts_yn": "Y", // 공매도 가능 여부 또는 관련 상태 여부
        "stck_shrn_iscd": "108320", // 종목 단축코드
        "fcam_cnnm": "500", // 액면가 한글 표기용 값
        "cpfn_cnnm": "81 억", // 자본금 한글 표기용 값
        "frgn_hldn_qty": "4533731", // 외국인 보유 수량

        "vi_cls_code": "N", // VI 발동 여부/구분 코드
        "ovtm_vi_cls_code": "N", // 시간외 VI 발동 여부/구분 코드
        "last_ssts_cntg_qty": "31627", // 최종 공매도 체결 수량 또는 관련 수량
        "invt_caful_yn": "N", // 투자유의 여부
        "mrkt_warn_cls_code": "00", // 시장경고 구분 코드
        "short_over_yn": "N", // 단기과열 여부
        "sltr_yn": "N", // 정리매매 여부
        "mang_issu_cls_code": "N" // 관리종목 여부/구분 코드
      },

      "rt_cd": "0", // KIS 업무 처리 결과 코드
      "msg_cd": "MCA00000", // 메시지 코드
      "msg1": "정상처리 되었습니다." // 처리 결과 메시지
    }
  }
}
```
