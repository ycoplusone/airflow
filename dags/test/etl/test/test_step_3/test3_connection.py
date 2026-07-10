from datetime import datetime
from airflow.decorators import dag, task
# ⭐️ Airflow의 Connection 금고에 접근하기 위한 기본 Hook을 가져옵니다.
from airflow.hooks.base import BaseHook

@dag(
    dag_id="test3_connection",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)
def my_connection_pipeline():

    @task
    def read_db_connection():
        # ⭐️ 중요: 웹 UI에서 만든 'mysql_2026' 금고를 통째로 낚아챕니다!
        conn = BaseHook.get_connection("mysql_2026")
        
        # 코드에 패스워드나 아이디를 노출하지 않고 안전하게 출력 및 사용이 가능합니다.
        print("🔒 [보안 통신 시작] Airflow 금고에서 접속 정보를 안전하게 불러왔습니다.")
        print(f"🌐 접속할 Host 주소: {conn.host}")
        print(f"👤 접속 계정 ID: {conn.login}")
        print(f"🔑 암호화된 비밀번호 길이: {len(conn.password)}자리")
        
    read_db_connection()

my_connection_pipeline()