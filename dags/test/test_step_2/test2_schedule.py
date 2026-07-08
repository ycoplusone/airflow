from datetime import datetime
from airflow.decorators import dag, task

# ⭐️ schedule에 '*/5 * * * *'를 넣어 5분마다 실행되도록 스케줄을 잡습니다.
# ⭐️ catchup=False는 이 DAG를 켜는 순간, 과거(6월 1일~오늘)의 밀린 작업들을 
#     한 번에 와다다 실행하지 말고 "지금부터만 잘 돌려라"라는 뜻입니다. (매우 중요!)
@dag(
    dag_id="test2_schedule",
    start_date=datetime(2026, 6, 1),
    schedule="*/5 * * * *",  # ⏱️ 5분마다 자동 실행!
    catchup=False,           # ⚠️ 과거 밀린 작업 실행 방지
)
def my_scheduled_pipeline():

    @task
    def print_current_time():
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"⏰ 현재 시간은 [{now}] 입니다. 스케줄러가 나를 깨웠습니다!")

    print_current_time()

my_scheduled_pipeline()