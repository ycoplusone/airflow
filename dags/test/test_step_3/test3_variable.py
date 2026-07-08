from datetime import datetime
from airflow.decorators import dag, task
# ⭐️ Airflow의 전역 변수 보관함에 접근하기 위해 Variable 클래스를 가져옵니다.
from airflow.models import Variable

@dag(
    dag_id="test3_variable",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)
def my_variable_pipeline():

    @task
    def read_global_variable():
        # ⭐️ 중요: Variable.get("Key이름")으로 웹 UI에 저장한 값을 쏙 빼옵니다.
        Variable.set(key="last_processed_date", value="2026-07-07")
        api_url = Variable.get("last_processed_date")
        
        
        print("공통 전역 변수를 보관함에서 성공적으로 불러왔습니다.")
        print(f"📡 현재 파이프라인이 데이터를 수집할 목적지 URL: [{api_url}]")

    read_global_variable()

my_variable_pipeline()