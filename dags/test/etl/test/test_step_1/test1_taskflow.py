from datetime import datetime
from airflow.decorators import dag, task # ⭐️ 오퍼레이터 대신 데코레이터를 가져옵니다.

# 1. @dag 데코레이터로 이 함수 자체가 하나의 DAG임을 선언합니다.
@dag(
    dag_id="test1_taskflow",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)

def my_taskflow_pipeline():

    # 2. @task 데코레이터를 붙이면 순수 파이썬 함수가 자동으로 Airflow 태스크가 됩니다!
    @task(task_id="push_number_task")
    def push_number():
        raw_value = 123
        print(f"📤 [TaskFlow] 숫자 {raw_value}을 생성했습니다.")
        return raw_value

    # 3. 받아오는 함수도 @task만 붙이면 끝! ti(Task Instance)를 복잡하게 부를 필요가 없습니다.
    @task(task_id="pull_number_task")
    def pull_and_add_one(pulled_value): # ⭐️ 일반 파이썬 함수처럼 매개변수로 직접 받습니다!
        final_result = pulled_value + 1
        print(f"📥 [TaskFlow] 앞선 태스크에서 {pulled_value}을 받아 1을 더했습니다: [{final_result}]")

    # 4. ⭐️ 획기적인 흐름 제어 및 XCom 연동
    # 오퍼레이터를 묶거나 xcom_pull을 명시할 필요 없이, 파이썬 함수 호출하듯 연결하면
    # Airflow가 뒷단에서 자동으로 XCom Push/Pull과 태스크 실행 순서(>> 역할을 자동으로)를 처리합니다!
    returned_data = push_number()
    pull_and_add_one(returned_data)

# DAG 가동하기
my_taskflow_pipeline()