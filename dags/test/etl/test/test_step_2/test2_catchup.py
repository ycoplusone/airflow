from datetime import datetime
from airflow.decorators import dag, task

# ⭐️ 일부러 start_date를 며칠 전인 2026년 6월 15일로 잡고,
# ⭐️ schedule을 '@daily'(매일 자정)로 설정한 뒤, catchup을 True로 켜보겠습니다!
@dag(
    dag_id="test2_catchup",
    start_date=datetime(2026, 6, 15),  # 수일 전 과거 날짜
    schedule="@daily",                 # 매일 한 번씩 실행
    catchup=True,                      # ⚠️ 과거 밀린 작업을 모두 찾아내서 실행해라!
)
def my_catchup_pipeline():

    @task
    def print_history():
        print("정상 가동 중입니다!")

    print_history()

my_catchup_pipeline()