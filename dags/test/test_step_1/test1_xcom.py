from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. 데이터를 생성하고 '리턴(자동 XCom Push)'하는 첫 번째 함수
def push_number():
    raw_value = 123
    print(f"📤 [태스크 1] 숫자 {raw_value}을 생성했습니다. 우체통에 넣습니다!")
    return raw_value  # ⭐️ 파이썬의 return 구문은 Airflow가 자동으로 XCom 우체통에 넣어줍니다.

# 2. 앞선 태스크의 데이터를 'Pull(가져오기)'해서 가공하는 두 번째 함수
# ⭐️ ti(Task Instance) 객체를 매개변수로 받으면 Airflow의 우체통에 접근할 수 있습니다.
def pull_and_add_one(ti):
    # 'push_number_task'라는 ID를 가진 태스크가 우체통에 넣은 값을 쏙 빼옵니다!
    pulled_value = ti.xcom_pull(task_ids="push_number_task")
    
    final_result = pulled_value + 1
    print(f"📥 [태스크 2] 우체통에서 {pulled_value}을 꺼내 1을 더했습니다: [{final_result}]")

# 3. DAG 정의
with DAG(
    dag_id="test1_xcom",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 첫 번째 태스크: 숫자 던지기
    task_push = PythonOperator(
        task_id="push_number_task",
        python_callable=push_number,
    )

    # 두 번째 태스크: 숫자 받아서 1 더하기
    task_pull = PythonOperator(
        task_id="pull_number_task",
        python_callable=pull_and_add_one,
    )

    # 흐름 지정: 던지고 -> 받기
    task_push >> task_pull