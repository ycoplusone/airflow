from datetime import datetime
from airflow import DAG
# 이번에는 PythonOperator를 불러옵니다!
from airflow.providers.standard.operators.python import PythonOperator

# 1. 실제 비즈니스 로직이 담긴 파이썬 함수들을 만듭니다.
def start_lesson():
    print("👨‍🏫 선생님: 자, 파이썬 오퍼레이터 수업 시작합니다!")

def calculate_magic(number):
    result = number * 10
    print(f"🧙‍♂️ 계산기: 입력받은 {number}에 10을 곱하면 {result}입니다!")
    return result

def end_lesson():
    print("🎉 학생: 와! 파이썬 코드가 Airflow에서 직접 실행됐어요!")

# 2. DAG 정의
with DAG(
    dag_id='my_second_python_dag',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['tutorial', 'python']
) as dag:

    # 3. PythonOperator로 파이썬 함수들을 작업(Task)으로 등록합니다.
    task_1 = PythonOperator(
        task_id='lesson_start',
        python_callable=start_lesson # 실행할 함수 이름을 적어줍니다.
    )

    task_2 = PythonOperator(
        task_id='lesson_calculate',
        python_callable=calculate_magic,
        op_args=[5] # 함수에 매개변수(인자)를 던져줄 때 사용합니다 (number=5)
    )

    task_3 = PythonOperator(
        task_id='lesson_end',
        python_callable=end_lesson
    )

    # 4. 순서 정하기
    task_1 >> task_2 >> task_3