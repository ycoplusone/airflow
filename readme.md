# Airflow 기반 데이터 파이프라인 개발 프로젝트

이 문서는 Airflow를 사용하여 데이터 파이프라인을 개발하고 운영하기 위한 가이드와 개발 일지를 기록합니다.

---

## 1. 프로젝트 개요

- **목표:** 사내 데이터 레이크하우스 구축을 위한 ETL/ELT 파이프라인 개발 및 검증
- **주요 과제:** DAG의 안정적인 스케줄링, 태스크 에러 핸들링, 데이터 정합성 검증 및 운영 안정성 확보

## 2. 기술 스택

- **Airflow Version:** 3.2.2
- **Python Version:** 3.12.13
- **Executor:** LocalExecutor
- **Metadata DB:** airflow.db (SQLite)
- **주요 Provider:** `apache-airflow-providers-mysql`, `apache-airflow-providers-http`

## 3. 로컬 개발 환경 설정

로컬에서 Airflow를 실행하고 DAG를 테스트하기 위한 환경 설정 절차입니다.

### 3.1. 시스템 의존성 설치 (for MySQL)

```bash
# 패키지 매니저 업데이트
sudo apt update

# Python MySQL 드라이버 빌드에 필요한 패키지 설치
sudo apt install -y default-libmysqlclient-dev build-essential pkg-config
```

### 3.2. Python 패키지 설치

```bash
# 순수 Python MySQL 드라이버
pip install pymysql

# Airflow MySQL 프로바이더 (의존성 없이)
pip install apache-airflow-providers-mysql --no-deps

# MySQL 클라이언트
pip install mysqlclient

# HTTP 프로바이더
pip install apache-airflow-providers-http
```

### 3.3. Airflow 실행

```bash
# 가상환경 활성화 및 Airflow 홈 디렉토리 설정
source venv/bin/activate
export AIRFLOW_HOME=~/airflow_2026/airflow_2026

# 인증 없이 API 및 웹서버 접근 허용 (개발용)
export AIRFLOW__API__AUTH_BACKENDS="airflow.providers.fab.auth_manager.api.auth.backend.no_auth"
export AIRFLOW__WEBSERVER__AUTH_BACKEND="airflow.providers.fab.auth_manager.api.auth.backend.no_auth"

# Airflow 웹서버에서 설정 노출
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Airflow Standalone 모드로 실행 (웹서버 + 스케줄러)
nohup airflow standalone > airflow_standalone.log 2>&1 &
```

## 4. 개발 일지 및 초기 테스트 결과

- 작업(DAG) 실행 간 몇 초 정도의 지연이 관찰됨. (`poke_interval` 파라미터로 조정 가능성 확인)
- 기존 상용 ETL 도구와 같이 데이터 처리 파이프라인으로 활용 가능함을 확인함.
- Python 기반으로 확장이 용이하여 거의 모든 시스템과 연동 가능할 것으로 기대됨.
- **추후 테스트 항목:** AWS S3에 Parquet 포맷으로 데이터를 생성하고, 이를 AI/IoT 데이터 처리 파이프라인과 연계하는 시나리오 검증 필요.
- Git 연동 시 간헐적으로 충돌 또는 설정 문제가 발생하여 주의가 필요함.

***********************************************************

## 6. 일자별 작업 내역

*이 섹션에는 각 날짜별 주요 작업 내용, 이슈, 해결 과정 등을 기록합니다.*
### 2026-07-15
- **작업:** task01 작업 전체 생성 및 테스트
- **이슈:** 개발후 수행 이력 없는 dag를 콜해서 수행할시 hang 발생
- **결과:** 일단 개별 수행 테스트 후 task01_main 으로 통합 하여 수행 하니 해소됨, 확실한 원인과 해소 방법은 따로 찾을수 없었다. 

### 2026-07-14-2
- **작업:** df_marco_info.py 생성
- **이슈:** 기존 mysql 5.5 에서 charset(utf8mb4) 차이로 오류 발생
- **결과:** {"charset": "utf8"} 를 connection 에 추가 함으로써 해소.

### 2026-07-14-1
- **작업:** mysql to mysql 작업 시작
- **이슈:** 작업중 실수로 메타데이터 파일을 삭제 하여 이전의 connection 값을 모두 삭제함.
- **결과:** airflow connections export connections.json 으로 백업수 있는것을 확인.

### 2026-07-10
- **작업:** SMTP 메일 발송 테스트
- **이슈:** connection 에서 SMTP 를 이용한 발송테스트는 실패
- **결과:** `smtplib` 를 이용한 전환 완료

### 2026-07-08
- **작업:** 텔레그램 메세지 발송 테스트
- **이슈:** Connection 의 일반(`BaseHook`) 을 이용한 메세지 발송 테스트
- **결과:** 정상 발송 테스트 완료

## 2026-07-06
- **작업:** `etl_job_test` TriggerDagRunOperator를 포함한 dag를 통합 수행하는 작업 
- **이슈:** 흐름중 오류가 발생하면 전체가 중단되는것을 방지하고 그냥 진행하도록 처리함과 동시에 오류 로그를 기록하도록함.
- **결과:** 정상 처리

## 2026-07-03
- **작업:** `etl_wf_test1` 하나의 중간 관리 단위인 WF를 생성하고 테스트
- **이슈:** 흐름중 오류가 발생하면 전체가 중단되는것을 방지하고 그냥 진행하도록 처리함과 동시에 오류 로그를 기록하도록함.
- **결과:** 정상 처리

## 2026-07-01
- **작업:** `etl_test*` 직접수행되는 말단 테스트
- **이슈:** 작업의 수행 시작과 종료 혹은 오류를 db에 기록하는 기준 마련
- **결과:** 정상 처리

## 2026-06-26
- **작업:** `callback,dynamic,task_Group` 테스트 진행
- **이슈:** 기본 가이드에서 제공하는 기준으로 처리
- **결과:** 이상 없음.

## 2026-06-25
- **작업:** `connection , variable` 테스트 진행
- **이슈:** 기본 가이드에서 제공하는 기준으로 처리
- **결과:** 이상 없음.

## 2026-06-24
- **작업:** `branch,catchup,schedule,trigger_rule` 테스트 진행
- **이슈:** 기본 가이드에서 제공하는 기준으로 처리
- **결과:** 이상 없음.

## 2026-06-23
- **작업:** `bash , python , taskflow , xcom` 테스트 진행
- **이슈:** 기본 가이드에서 제공하는 기준으로 처리
- **결과:** 이상 없음.

## 2026-06-19
- **작업:** wsl 설치및 airflow 설치및 진행
- **이슈:** 기본 가이드에서 제공하는 기준으로 처리
- **결과:** 이상 없음.