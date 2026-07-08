**테스트 후기**
1. 작업(dag) 간의 약간의 지연(몇초) 정도 있다.(poke_interval 로 해소가능) 
2. 기존 ETL Tool 처럼 사용이 물론 가능하다
3. 확장성은 훌륭하다. 모든것에 연결해서 처리 할수 있다.
4. AWS S3, parquet , AI , IOT 에서 포맷으로 데이터 생성하는것을 테스트 해야 한다

===========================================================
## mysql 연결 관련 가이드

# 1. 리눅스 패키지 매니저를 최신 상태로 새로고침합니다.
sudo apt update

# 2. 파이썬 mysql 드라이버가 빌드될 때 요구하는 리눅스 핵심 부품들을 통째로 설치합니다.
sudo apt install -y default-libmysqlclient-dev build-essential pkg-config

# 3. 컴파일 없이 1초 만에 깔리는 순수 파이썬 MySQL 드라이버 설치
pip install pymysql

# 4. 에러가 났던 기본 드라이버를 생략하고 Airflow의 MySQL 프로바이더만 강제 설치
pip install apache-airflow-providers-mysql --no-deps
    - 위 파일 설치에서 오류 발생시 => sudo apt install -y libmysqlclient-dev
# 5. mysql 클라이언트 설치
pip install mysqlclient
=============================================================    
    


===================================================================================
# 리녹스 접근 
    cmd
    wsl
    cd ~/airflow_2026
    source venv/bin/activate
    export AIRFLOW_HOME=~/airflow_2026/airflow_2026
    - 종료 => deactivate


# 환경설정 
export AIRFLOW__API__AUTH_BACKENDS="airflow.providers.fab.auth_manager.api.auth.backend.no_auth"
export AIRFLOW__WEBSERVER__AUTH_BACKEND="airflow.providers.fab.auth_manager.api.auth.backend.no_auth"
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True    
# 환경설정 검색
env | grep AIRFLOW


# airflow standalone -- 스케줄과 서버 동시 실행
nohup airflow standalone  > airflow_standalone.log 2>&1 &

# airflow standalone 프로세스 조회 및 킬
ps -ef | grep "airflow standalone"
pkill -f airflow


# 초기화
# airflow db migrate
#
# airflow 스케줄러 동작
# nohup airflow scheduler > scheduler.log 2>&1 &
#
# airflow 스케줄러 킬
# ps -ef | grep "airflow standalone"
# pkill -f "airflow standalone"
#
# airflow 시작
# airflow api-server --port 8080
