from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# 1. DAG(파이프라인)의 기본 설정(방향과 주기)을 정의합니다.
with DAG(
    dag_id="test1_bash_operator",  # 웹 UI에 표시될 고유한 이름
    start_date=datetime(2026, 6, 1),      # 언제부터 이 파이프라인을 가동할지 설정
    schedule=None,                        # 주기적 실행 없이 '수동 트리거'로만 테스트하겠다는 의미
    catchup=False,                        # 누적된 과거 작업 실행 안 함
) as dag:

    # 2. 첫 번째 태스크: 단순 문자열 출력하기
    task_echo_hello = BashOperator(
        task_id="print_hello_wsl",
        bash_command='echo "안녕하세요! WSL2 터미널에서 실행 중입니다."',
    )

    # 3. 두 번째 태스크: 리눅스 명령어로 현재 위치에 임시 텍스트 파일 만들기
    task_create_file = BashOperator(
        task_id="create_temp_file",
        bash_command='echo "Airflow가 이 글을 씁니다!!!@#." > ~/airflow_2026/airflow_test_result.txt',
    )

    # 4. 태스크 간의 실행 순서(흐름) 지정하기 (>> 기호 사용)
    # 문자열 출력을 성공적으로 마치면 -> 텍스트 파일을 생성해라!
    task_echo_hello >> task_create_file