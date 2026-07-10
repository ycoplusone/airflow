from datetime import datetime
import random
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

@dag(
    dag_id="test2_trigger_rule",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)
def my_trigger_rule_pipeline():

    @task
    def pick_number():
        number = random.randint(1, 100)
        return number

    @task.branch
    def check_even_odd(number):
        if number % 2 == 0:
            return "even_way"
        return "odd_way"

    @task(task_id="even_way")
    def even_way():
        return "🔵 짝수 처리 완료"

    @task(task_id="odd_way")
    def odd_way():
        return "🔴 홀수 처리 완료"

    # trigger_rule 설정을 통해 한쪽이 스킵되어도 하나만 성공하면 가동하도록 합니다.
    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def close_pipeline(msg):
        print(f"🏁 [최종 마감] 전달받은 메시지: {msg}")
        print("🎉 모든 파이프라인 작업이 안전하게 종료되었습니다.")

    # 뼈대 연동 수정 부분
    num = pick_number()
    branch_way = check_even_odd(num)
    
    even_res = even_way()
    odd_res = odd_way()
    
    # 철길을 명확하게 한 줄씩 연결해 줍니다.
    branch_way >> even_res
    branch_way >> odd_res
    
    # 살아남은 쪽의 결과가 최종 마감 태스크로 인자(Argument)로 안전하게 흘러들어갑니다.
    even_res >> close_pipeline(even_res)
    odd_res >> close_pipeline(odd_res)

my_trigger_rule_pipeline()