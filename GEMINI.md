# Airflow Development & Testing Guidelines

## 1. 프로젝트 개요 및 목적
- **목적:** [예: 사내 데이터 레이크하우스 구축을 위한 ETL/ELT 파이프라인 개발 및 검증]
- **테스트 목적:** DAG의 정상 스케줄링 여부, 태스크 에러 핸들링, 데이터 정합성 검증 및 운영 안정성 테스트.

## 2. 기술 스택 및 버전
- **Airflow Version:** 3.2.2
- **Python Version:** 3.12.13
- **Executor:** LocalExecutor
- **주요 오퍼레이터:** [예: PythonOperator, BashOperator, DockerOperator, 이외에 자주 쓰는 Provider (예: 다양한 DB 오퍼레이터, DatabricksOperator 등)]
- **Metadata DB:** airflow.db

## 3. DAG 작성 규칙 (Coding Convention)
- **DAG 선언 방식:** `with DAG(...)` 컨텍스트 매니저 방식을 기본으로 사용합니다.
- **기본 인수 설정:** 모든 DAG에는 `owner`, `retries` 등을 포함한 `default_args`를 설정하는 것을 권장합니다.
- **Task 선언 방식:** TaskFlow API (`@task` 데코레이터) 사용을 [권장/제한]합니다. (기존 Operator 방식과 통일 필요)
- **태스크 의존성 정의:** 비트 시프트 연산자(`>>`, `<<`)를 사용하여 명확하게 시각화합니다.
- **Id 규칙:** `dag_id`와 `task_id`는 하이픈(`-`) 대신 언더바(`_`)를 사용하는 snake_case를 준수합니다. (예: `extract_user_logs`)


## 4. 운영 및 환경 분리 규칙
- **환경 변수 제어:** Local, Staging, Production 환경은 Airflow Variables 또는 환경 변수(`os.environ`)를 통해 동적으로 분기하도록 코드를 작성합니다.
- **Connection 관리:** 모든 외부 시스템(DB, API 등) 연동은 하드코딩하지 않고, Airflow Connection ID(`conn_id`)를 참조합니다.
- **Catchup 및 스케줄링:** 과거 데이터를 재처리하는 특별한 케이스가 아니면 기본적으로 `catchup=False`로 설정합니다.

## 5. 테스트 및 검증 가이드 (Testing Rules)
- **로컬 검증 방법:** DAG 코드를 수정한 후에는 반드시 CLI에서 `airflow dags list-import-errors` 또는 `pytest`를 통해 Syntax 및 Import 에러가 없는지 검증합니다.
- **Task 단위 테스트:** 특정 태스크의 오동작을 줄이기 위해 `airflow tasks test [dag_id] [task_id] [execution_date]` 명령어로 단일 태스크 동작을 사전 검증하도록 유도합니다.
- **상태 저장(XCom) 제한:** 태스크 간 대용량 데이터 전송을 위해 XCom을 사용하는 것을 금지합니다. 대용량 데이터는 스토리지(S3, 로컬 경로 등)에 저장하고 경로만 XCom으로 공유합니다.

## 6. 에러 핸들링 및 알림 (Alerting)
- **재시도 규칙:** 네트워크 불안정 등을 대비해 모든 Task에는 최소 `retries: 2`, `retry_delay: timedelta(minutes=5)`를 기본 `default_args`로 설정합니다.
  - **주의:** Airflow 기본 설정(`default_task_retries`)은 0이므로, `default_args`를 통해 반드시 재정의해야 합니다.
- **실패 알림:** DAG 또는 중요 Task 실패 시 슬랙(Slack) 이메일 알림을 보낼 수 있도록 `on_failure_callback` 정의 규칙을 포함합니다.