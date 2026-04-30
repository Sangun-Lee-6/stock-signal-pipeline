## DuckDB는 왜 append-only INSERT에서도 lock이 걸리는가?

---

### 🔹 append-only 구조여도 lock이 걸리는 이유

- 현재 프로젝트는 주가/기사 스냅샷을 계속 INSERT만 하고 있음 ⇒ append-only 구조
- 그럼에도 lock이 걸리는 이유는 row-level lock이 아니라 process-level write lock 때문

---

### 🔹 파일 오픈 단계에서 lock이 걸림

- DuckDB는 서로 다른 테이블에 INSERT하는지 여부를 보기 전에, 두 프로세스가 같은 DB 파일을 쓰기 가능한 상태로 열려고 하고, 이 시점에서 파일 lock이 걸림
- SQL 실행부가 아니라 DB 파일 오픈 단계에서 lock이 걸림

---

### 🔹 DuckDB 파일 잠금 동작 흐름

- DuckDB는 DB 파일을 열 때 사용할 file flags를 생성(`READ_LOCK` 또는 `WRITE_LOCK`)
  - 그리고 같은 파일에서 새 DB를 만들거나 기존 DB를 열 때, 위에서 만든 flags로 실제 파일을 오픈
    - → `fs.OpenFile(path, flags)`
- 파일을 열 때 파일 잠금을 설정하고, lock 획득에 실패하면 에러 발생
  - → `rc = fcntl(fd, F_SETLK, &fl);`
  - → 열려있는 파일(`fd`)에 대해 잠금 설정(`fl`)을 OS에 요청하고, 그 성공/실패 결과를 rc에 저장

```
1. DuckDB가 database.duckdb 파일을 연다.
2. 열린 파일에 대해 fd가 생긴다.
3. DuckDB가 READ_LOCK 또는 WRITE_LOCK 정보를 fl에 담는다.
4. fcntl(fd, F_SETLK, &fl)로 OS에 파일 잠금을 요청한다.
5. 이미 다른 프로세스가 write lock을 잡고 있으면 rc == -1로 실패한다.
```

---
