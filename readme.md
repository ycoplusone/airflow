# Airflow 데이터 파이프라인 프로젝트

## 📖 개요

이 프로젝트는 Apache Airflow를 사용하여 데이터 파이프라인을 관리하고 실행하기 위한 중앙 저장소입니다. 이 문서는 프로젝트의 구조, 로컬 개발 환경 설정, 새로운 DAG 작성 및 배포 방법에 대한 가이드를 제공합니다.

---

## 📂 프로젝트 구조

프로젝트는 다음과 같은 표준 구조를 따릅니다. 각 디렉토리와 파일의 역할은 아래와 같습니다.

-   **`dags/`**: 모든 Airflow DAG 파이썬 파일이 이곳에 위치합니다. Airflow 스케줄러는 이 디렉토리를 주기적으로 스캔하여 DAG를 업데이트합니다.
-   **`plugins/`**: 직접 개발한 Operator, Hook, Sensor 등 커스텀 플러그인을 저장합니다.
-   **`include/`**: DAG 로직과 분리하고 싶은 파일들(예: SQL 템플릿, 설정 파일, 작은 데이터 파일)을 보관합니다.
-   **`tests/`**: DAG의 유효성, 커스텀 오퍼레이터의 동작 등을 검증하는 테스트 코드를 작성합니다.
-   **`docker-compose.yml`**: Docker를 사용하여 Airflow 웹서버, 스케줄러, 데이터베이스 등 로컬 개발 환경을 한번에 실행하기 위한 설정 파일입니다.
-   **`Dockerfile`**: `requirements.txt`에 명시된 라이브러리 설치 등 프로젝트에 필요한 구성을 포함하는 커스텀 Airflow 이미지를 빌드합니다.
-   **`requirements.txt`**: 프로젝트에 필요한 Python 패키지 목록입니다. Docker 이미지 빌드 시 사용됩니다.
-   **`.env.example`**: 로컬 환경 설정에 필요한 환경 변수 예시입니다. 실제 사용 시에는 `.env` 파일로 복사하여 값을 채워야 합니다.

---

## 🚀 로컬 개발 환경 설정

Docker Compose를 사용하여 로컬에서 Airflow 환경을 쉽게 구성하고 실행할 수 있습니다.

**사전 요구사항:**
*   Docker
*   Docker Compose

**설정 단계:**

1.  **.env 파일 생성**
    `.env.example` 파일을 복사하여 `.env` 파일을 생성하고, 필요에 따라 내부 변수를 수정합니다.

    ```bash
    cp .env.example .env
    ```

2.  **필요한 디렉토리 생성**
    `docker-compose.yml`에서 마운트할 로컬 디렉토리를 미리 생성합니다.

    ```bash
    mkdir -p ./logs ./plugins ./dags
    ```

3.  **Airflow 초기화 및 실행**
    Docker Compose를 사용하여 Airflow 서비스를 빌드하고 실행합니다.

    ```bash
    # Airflow 데이터베이스 초기화 및 관리자 계정 생성
    docker-compose up airflow-init

    # 모든 Airflow 서비스 시작 (백그라운드 실행)
    docker-compose up -d
    ```

4.  **Airflow UI 접속**
    웹 브라우저에서 `http://localhost:8080`으로 접속합니다. `docker-compose up airflow-init` 실행 시 터미널에 출력된 사용자 정보(기본값: `airflow` / `airflow`)로 로그인할 수 있습니다.

5.  **환경 종료**
    실행 중인 Airflow 환경을 종료하려면 다음 명령어를 사용합니다.

    ```bash
    docker-compose down
    ```

---

## ✨ 주요 작업 가이드

### 새로운 DAG 추가하기

1.  새로운 파이썬 파일(`my_new_dag.py`)을 `dags/` 디렉토리 안에 생성합니다.
2.  Airflow의 기본 개념에 따라 DAG를 작성합니다.
    -   `default_args`를 정의하여 Task에 공통 인자를 제공합니다.
    -   `@dag` 데코레이터 또는 `with DAG(...) as dag:` 구문을 사용하여 DAG를 정의합니다.
    -   Task 간의 의존성은 `>>` 또는 `<<` 연산자로 설정합니다.
3.  파일을 저장하면 Airflow 스케줄러가 자동으로 DAG를 인식하여 UI에 표시합니다. (약 30초 ~ 1분 소요)

### Airflow 변수(Variables) 및 연결(Connections) 사용하기

-   **Variables**: API 키, 경로 등 정적인 값을 저장할 때 사용합니다. Airflow UI의 `Admin -> Variables` 메뉴에서 생성하거나, 코드 내에서 `Variable.get("my_var")`으로 조회할 수 있습니다.
-   **Connections**: 외부 시스템(DB, API 등)에 대한 연결 정보를 저장합니다. Airflow UI의 `Admin -> Connections` 메뉴에서 생성하며, Hook에서 connection ID를 사용하여 쉽게 연결 정보를 가져올 수 있습니다.

> **보안 참고**: 실제 운영 환경에서는 민감한 정보(비밀번호, API 키 등)를 Airflow Variables/Connections에 직접 저장하기보다 HashiCorp Vault, AWS Secrets Manager 등 외부 Secret Backend를 연동하여 사용하는 것을 강력히 권장합니다.

