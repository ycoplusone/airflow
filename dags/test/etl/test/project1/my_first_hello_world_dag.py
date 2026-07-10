from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


# 1. DAG의 기본 설정(옵션)을 정의합니다.
default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. 파이프라인(DAG)의 뼈대를 정의합니다.
with DAG(
    dag_id='my_first_hello_world_dag',  # 웹 화면에 표시될 DAG의 고유 이름
    default_args=default_args,
    description='선생님과 함께 만드는 첫 번째 파이프라인',
    schedule=None,                      # 자동 스케줄링 없이 수동으로 실행 테스트
    start_date=datetime(2026, 1, 1),    # DAG가 활성화될 기준 시작 날짜
    catchup=False,                      # 과거 밀린 작업들을 소급 적용해서 실행할지 여부
    tags=['tutorial'],
) as dag:

    # 3. 오퍼레이터(Operator)를 이용해 실제 수행할 작업(Task)들을 만듭니다.
    task_start = BashOperator(
        task_id='print_start',
        bash_command='echo "Airflow 파이프라인 시작합니다!"',
    )

    task_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello World! 나는 이제 Airflow 마스터다!"',
    )

    task_end = BashOperator(
        task_id='print_end',
        bash_command='echo "파이프라인이 성공적으로 끝났습니다!"',
    )

    # 4. 작업들 간의 실행 순서(의존성)를 지정합니다.
    # task_start가 끝나면 task_hello가 실행되고, 그다면 task_end가 실행됩니다.
    task_start >> task_hello >> task_end