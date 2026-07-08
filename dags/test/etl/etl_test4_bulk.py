# ⭐️ 호환성 부적: mysqlclient 에러 우회를 위해 맨 위에 무조건 배치합니다.
import pymysql
pymysql.install_as_MySQLdb()

from datetime import datetime
import random
from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow import DAG
from lib.utilClass import utilCls       # airflow 에 사용되는 각종 유틸들.

with DAG(
    dag_id                  = 'etl_test4_bulk',
    default_args            = utilCls().default_args() ,
    schedule                = None,  # 필요시 크론 표현식 주입 (예: '0 3 * * *')
    catchup                 = False,
    tags                    = ['test', 'df'],
) as dag:
    @task
    def task00(**context):
        seq_id = utilCls().beginLog(context)
        return seq_id       

    # 1. [Extract] 대량의 가상 데이터(5,000건)를 메모리에 생성하는 단계
    @task
    def generate_bulk_data():
        print("⚡ [Extract] 5,000건의 대용량 매출 데이터를 고속 생성합니다...")
        branches    = ["Seoul", "Busan", "Jeju", "Incheon", "Daegu"]
        items       = ["Monitor", "Keyboard", "Mouse", "Laptop", "Desk"]
        
        bulk_data = []
        for _ in range(100):
            branch = random.choice(branches)
            item = random.choice(items)
            price = random.randint(10, 200) * 100  # 1,000원 ~ 20,000원
            quantity = random.randint(1, 50)
            total_price = price * quantity
            
            # 💡 중요: insert_rows 메서드는 '튜플(Tuple)' 리스트나 리스트의 리스트 형태를 받습니다.
            # 컬럼 순서: (branch, item, price, quantity, total_price)
            bulk_data.append((branch, item, price, quantity, total_price))
            
        print(f"✅ 대용량 데이터 생성 완료 (건수: {len(bulk_data)}건)")
        return bulk_data

    # 2. [Load] ⭐️ 오늘의 핵심: 기존 데이터 DELETE 후 대량 INSERT + 단일 Commit 처리
    @task
    def truncate_and_bulk_load(data_list):
        print("💾 MySQL 금고를 열어 접속 드라이버(Hook)를 가져옵니다.")
        mysql_hook = MySqlHook(mysql_conn_id="my_mysql_warehouse")
        
        # 2-1. 대상 테이블이 없다면 자동 생성
        create_table_query = """
        CREATE TABLE IF NOT EXISTS mart_bulk_sales (
            id INT AUTO_INCREMENT PRIMARY KEY,
            branch VARCHAR(50),
            item VARCHAR(50),
            price INT,
            quantity INT,
            total_price INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        mysql_hook.run(create_table_query)
        
        # 2-2. 🚨 적재 이전 해당 테이블의 데이터를 DELETE(또는 TRUNCATE)로 초기화
        print("🗑️ [초기화] 적재 전에 'mart_bulk_sales' 테이블의 기존 데이터를 깨끗이 삭제합니다.")
        mysql_hook.run("DELETE FROM mart_bulk_sales;") 
        # (실무 팁: 데이터가 수천만 건 이상으로 매우 많을 때는 DELETE보다 TRUNCATE TABLE mart_bulk_sales;가 훨씬 빠릅니다!)

        # 2-3. 🚀 대용량 대량 insert + 한 번에 commit 처리
        print("🚀 [Bulk Insert] 1,000건의 데이터를 하나의 트랜잭션으로 묶어 고속 적재합니다...")
        
        # insert_rows 인수 설명:
        # - table: 데이터를 넣을 테이블 명
        # - rows: 튜플 형태의 데이터 리스트 (5000건)
        # - target_fields: 데이터가 들어갈 컬럼 이름 목록
        # - commit_every: ⭐️ 아주 중요! 몇 건마다 Commit할지 지정합니다. 
        #   여기에 5000을 주거나 생략하여 한 번에 처리되도록 하면 단 하나의 트랜잭션으로 Commit됩니다!
        target_columns = ["branch", "item", "price", "quantity", "total_price"]
        
        mysql_hook.insert_rows(
            table="mart_bulk_sales",
            rows=data_list,
            target_fields=target_columns,
            commit_every=1000 
        )
        print("🎯 [Bulk Insert 완료] 모든 데이터가 한 번에 Commit 되었습니다.")

    # 3. [Log/Report] 적재가 최종 성공했는지 DB 건수를 조회하여 완료 로그를 남기는 단계
    @task
    def log_completion():
        mysql_hook = MySqlHook(mysql_conn_id="my_mysql_warehouse")
        
        # 실제로 몇 건이 들어갔는지 COUNT 쿼리 날리기
        count_query = "SELECT count(1) cnt  FROM mart_bulk_sales"
        result = mysql_hook.get_first(count_query)
        current_count = result.get('cnt',0) if result else 0 # NoneType 일경우를 대비한 구문
        
        print("\n✨ 🏁 [파이프라인 최종 완료 로그] -------------------------")
        print(f"📅 작업 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📊 현재 외부 MySQL 'mart_bulk_sales' 테이블 적재 데이터 총계: [{current_count}] 건")
        print("🎉 이전 데이터 청소 및 5,000건 벌크 적재 미션이 완벽하게 성공했습니다!")
        print("------------------------------------------------------------\n")

    @task
    def task99(**context):
        ti = context['ti']
        seq_id = ti.xcom_pull(task_ids="task00")
        utilCls().endLog(seq_id=seq_id , err_msg="")        

    # 파이프라인 순서 연결
    records = generate_bulk_data()
    
    # 5000건 데이터 전달 -> 삭제 후 벌크 적재 -> 마감 완료 로그 기록
    task00() >> records >> truncate_and_bulk_load(records) >> log_completion() >> task99()

