from datetime import datetime
import random # 임의의 숫자를 뽑기 위한 파이썬 기본 라이브러리
from airflow.decorators import dag, task

@dag(
    dag_id="test2_branch",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)
def my_branch_pipeline():

    # 1. 1부터 100 사이의 랜덤 숫자를 하나 뽑아서 전달하는 태스크
    @task
    def pick_random_number():
        number = random.randint(1, 100)
        print(f"🎲 뽑힌 숫자는 [{number}] 입니다!")
        return number

    # 2. ⭐️ 오늘 수업의 주인공: 조건 분기 태스크 (@task.branch)
    @task.branch
    def check_even_or_odd(number):
        if number % 2 == 0:
            print(f"짝수네요! 'even_task_way'로 길을 틀겠습니다.")
            return "even_task_way" # 다음에 실행할 태스크 ID를 리턴!
        else:
            print(f"홀수네요! 'odd_task_way'로 길을 틀겠습니다.")
            return "odd_task_way"  # 다음에 실행할 태스크 ID를 리턴!

    # 3. 짝수일 때 실행될 최종 목적지 태스크
    @task
    def even_task_way():
        print("🔵 짝수 전용 철길에 도착했습니다. 작업을 완료합니다.")

    # 4. 홀수일 때 실행될 최종 목적지 태스크
    @task
    def odd_task_way():
        print("🔴 홀수 전용 철길에 도착했습니다. 작업을 완료합니다.")

    # 5. [중요] 데이터 흐름 및 파이프라인 뼈대 연결
    selected_num = pick_random_number()
    branch_decision = check_even_or_odd(selected_num)
    
    # 분기 태스크 뒤에 두 갈래 길을 리스트 형태로 연결해 줍니다.
    branch_decision >> [even_task_way(), odd_task_way()]

# @task 데코레이터 를 사용하고 명시적으로 task_id를 지시하지 않으면 함수명을 task_id로 사용된다.
my_branch_pipeline()