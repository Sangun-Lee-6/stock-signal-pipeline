## Airflow Dag에서 여러 프로세스의 의미

> `여러 프로세스`는 Dag 자체가 아니라, LocalExecutor가 task 실행을 위해 띄우는 별도 프로세스를 의미

---

### 🔹 Airflow에서 생성되는 프로세스

1. Dag parsing : Dag 파일을 읽고 Dag 객체로 변환
   - → 프로세스 단위는 Dag 파일 단위
2. Task execution : 실제 task를 실행
   - → 프로세스 단위는 executor 설정에 따라 다름

- Dag는 프로세스로 되는게 아니라 Python 코드로 정의된 워크플로우 객체

---

### 🔹 LocalExecutor

- LocalExecutor는 스케줄러 노드에서 프로세스를 복제(spawn)해서 task를 실행
- Dag 하나당 프로세스 하나가 생기는게 아니라, 실행되는 task instance들이 worker process에 할당됨

```
Airflow Scheduler Node
 ├─ scheduler process
 ├─ DAG processor process
 ├─ local worker process 1 → task A 실행
 ├─ local worker process 2 → task B 실행
 └─ local worker process 3 → task C 실행
```

---

### 🔹 DuckDB 동시성 관점에서 LocalExecutor

- DuckDB는 여러 프로세스가 같은 파일에 동시 쓰기가 안되는 점이 문제
- Airflow에서 하나의 Dag라도 여러 task가 서로 다른 프로세스에서 실행될 수 있음
  - 그리고 이 task들이 병렬 실행되면 동시 쓰기 구조가 될 수 있고 에러로 이어짐
  ```
  worker process 1 → mart.duckdb write
  worker process 2 → mart.duckdb write
  worker process 3 → mart.duckdb write
  ```
- 따라서 DuckDB에 쓰기 작업을 하는 task를 하나로 모아서 의존성을 걸어 순차 실행해야 함
