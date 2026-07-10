# 공식 Apache Airflow 이미지를 기반으로 합니다.
# 프로젝트에 맞는 Airflow 버전을 명시하는 것이 좋습니다.
FROM apache/airflow:2.8.1

# 루트 사용자로 전환하여 패키지를 설치합니다.
USER root

# requirements.txt 파일을 이미지 안으로 복사합니다.
COPY requirements.txt /requirements.txt

# pip를 사용하여 requirements.txt에 명시된 라이브러리들을 설치합니다.
RUN pip install --no-cache-dir -r /requirements.txt

# 다시 기본 사용자인 airflow로 전환합니다.
USER airflow