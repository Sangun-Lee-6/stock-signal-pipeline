## DuckDB의 구조

### 🔹 DuckDB의 구조

- DuckDB는 서버가 없는 embedded OLAP DB
  - → 서버가 없어서 여러 클라이언트의 읽기/쓰기를 조정할 수 없음
  - → 따라서 동시성 한계가 생김
- DuckDB는 별도 서버 프로세스 없이, 프로세스(ex. Python 애플리케이션 프로세스) 내부에 라이브러리로 로드되어 SQL 실행 엔진처럼 동작
  - → 배포가 단순함
  - → 여러 프로세스가 동시 쓰기 작업을 할 수 없음

---

### 🔹 DuckDB 구조의 어떤 점 때문에 동시성 한계가 발생하나?

- 동시성 한계의 원인 : 여러 쓰기 작업을 중앙에서 조율해주는 DB 서버가 없음
- DuckDB는 프로세스마다 DuckDB 엔진이 로드되어 실행됨
  - → 따라서 여러 프로세스가 같은 파일을 열 수는 있지만, 쓰기 작업을 조율해주는 서버가 없음
  ```
  Python Process + DuckDB Engine ── database.duckdb
  FastAPI Process + DuckDB Engine ── database.duckdb
  Airflow Process + DuckDB Engine ── database.duckdb
  ```
- 이러한 특성으로 인해 DuckDB는 동시성 모드를 2가지로 제한함
  - 읽기-쓰기 모드 : 하나의 프로세스가 읽기와 쓰기 가능
  - 읽기 전용 모드 : 여러 프로세스가 동시에 읽기 가능, 쓰기 불가

---

### 🔹 DuckDB는 왜 서버를 두지 않았을까

- DuckDB는 애플리케이션 안에서 바로 쓰는 분석 엔진을 목표로 설계됨
  - → 여러 애플리케이션이 동시에 접속하는 중앙 DB 서버가 아니라, 애플리케이션 프로세스 내부에 라이브러리로 로드되어 실행되는 embedded OLAP DB로 설계
  - → 따라서 DB 서버가 없음
- 다음과 같은 장점이 있음
  - 배포 단순성 : 별도 DBMS 설치/업데이트/운영 필요 없음
    - → 로컬, Python 코드, CLI에서 바로 사용 가능
  - 앱과 DB 사이 통신 비용 감소
  - 높은 이식성 : OS/CPU 의존성 낮음
  - 데이터 이동 최소화 : Pandas 같은 외부 데이터를 직접 쿼리 가능

---
