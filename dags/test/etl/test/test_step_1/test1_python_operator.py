from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Airflow가 대신 실행해 줄 '순수 파이썬 함수'들을 정의합니다.
def print_start_message():
    print("🚀 파이썬 오퍼레이터 학습을 시작합니다!")

# ⭐️ 중요: 매개변수(x, y)를 받아 곱셈을 하고 로그를 남기는 함수입니다.
def calculate_multiply(x, y):
    result = x * y
    print(f"🧙‍♂️ 계산기: 입력받은 {x}에 {y}를 곱하면 [{result}] 입니다!")
    return result

# 2. DAG 파이프라인 설정
with DAG(
    dag_id="test1_python_operator",
    start_date=datetime(2026, 6, 1),
    schedule=None,  # 수동 실행 테스트용
    catchup=False,
) as dag:

    # 3. 첫 번째 태스크: 단순 함수 실행
    task_start = PythonOperator(
        task_id="lesson_start",
        python_callable=print_start_message, # 실행할 함수명을 적어줍니다.
    )

    # 4. 두 번째 태스크: ⭐️ 매개변수를 함수에 던져주며 실행
    task_calc = PythonOperator(
        task_id="lesson_calculate",
        python_callable=calculate_multiply,
        op_args=[5, 10],  # 함수(calculate_multiply)의 x, y 자리에 순서대로 5와 10을 전달합니다!
    )

    # 5. 흐름 정하기: 시작 메시지를 찍고 나서 -> 계산기를 돌려라!
    task_start >> task_calc