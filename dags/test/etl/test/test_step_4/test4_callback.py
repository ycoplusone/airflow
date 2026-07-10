from datetime import datetime
from airflow.decorators import dag, task

# 1. 🚨 태스크가 실패했을 때 자동으로 호출될 '비상벨(콜백) 함수'를 정의합니다.
# 인자로 context(실패한 태스크의 정보들이 담긴 주머니)를 받습니다.
def my_emergency_alert(context):
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    
    print("🚨 [비상 공지] -----------------------------------------------")
    print(f"🔥 경고: [{task_id}] 태스크가 방금 실패했습니다!")
    print(f"⏰ 발생 시각: {execution_date}")
    print("📢 실무에서는 이 타이밍에 Slack API나 이메일 발송 코드가 실행됩니다!")
    print("------------------------------------------------------------")

# 2. DAG 정의 시, 온 실패 콜백 옵션을 전역으로 걸어줍니다.
@dag(
    dag_id="test4_callback",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    #on_failure_callback=my_emergency_alert, # ⭐️ DAG 내부의 어떤 태스크든 실패하면 이 함수를 실행해라!
)
def my_callback_pipeline():
# ⭐️ 힌트: task 데코레이터 내부에 실패 콜백을 직접 지정해 줍니다!
    
    @task
    def normal_task():
        print("🟢 나는 정상적으로 잘 끝나는 착한 태스크입니다.")


    @task( 
            task_id="dangerous_task",
            on_failure_callback=my_emergency_alert  # 이 태스크가 터지면 즉시 비상벨을 울려라! 
          )
    def dangerous_task():
        print("⚡ 위험한 작업을 시작합니다...")
        # ⭐️일부러 0으로 숫자를 나누어 파이썬 에러(ZeroDivisionError)를 강제로 발생시킵니다!
        bug_code = 10 / 0 
        return bug_code

    # 파이프라인 흐름: 착한 태스크가 끝나면 -> 시한폭탄 태스크 실행
    normal_task() >> dangerous_task()

my_callback_pipeline()