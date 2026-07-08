from datetime import datetime
import json
from airflow.decorators import dag, task
# ⭐️ 우리가 세팅한 MySQL 금고를 자유자재로 다룰 Hook을 가져옵니다.
from airflow.providers.mysql.hooks.mysql import MySqlHook

import pymysql
pymysql.install_as_MySQLdb()

from airflow import DAG
from lib.utilClass import utilCls       # airflow 에 사용되는 각종 유틸들.

with DAG(
    dag_id                  = 'etl_test0',
    default_args            = utilCls().default_args() ,
    schedule                = None,  # 필요시 크론 표현식 주입 (예: '0 3 * * *')
    catchup                 = False,
    tags                    = ['test', 'df'],
) as dag:
    @task
    def task00(**context):
        seq_id = utilCls().beginLog(context)
        return seq_id    


    # 1. [Extract] 외부 공공데이터나 파일에서 데이터를 긁어오는 단계 (가상 데이터 생성)    
    def fetch_raw_data():
        print("📥 마트 본사 시스템에서 오늘 자 원본 매출 데이터를 수집합니다.")
        # 가상의 마트 매출 Raw 데이터 (JSON 문자열 형태로 변환하여 전송)
        raw_sales = [
            {"branch": "Seoul", "item": "Apple", "price": 3000, "quantity": 10},
            {"branch": "Busan", "item": "Banana", "price": 1500, "quantity": "20"}, # quantity가 문자열로 꼬인 상황 가정
            {"branch": "Jeju", "item": "Orange", "price": 2500, "quantity": 5},
        ]
        return json.dumps(raw_sales)

    # 2. [Transform] 데이터의 타입을 맞추고 결측치를 청소하는 정제 단계
    @task
    def clean_retail_data():
        raw_data_json = fetch_raw_data()
        print("🧹 수집된 데이터를 깨끗하게 정제하고 정규화합니다.")
        raw_sales = json.loads(raw_data_json)
        cleaned_sales = []
        
        for record in raw_sales:
            # 문자열로 들어온 수량을 안전하게 정수(int)형으로 변환하는 정제 작업
            quantity = int(record["quantity"])
            total_price = record["price"] * quantity
            
            cleaned_sales.append({
                "branch": record["branch"],
                "item": record["item"],
                "price": record["price"],
                "quantity": quantity,
                "total_price": total_price
            })
            
        print(f"✨ 정제 완료된 데이터: {cleaned_sales}")
        return cleaned_sales

    # 3. [Load] ⭐️ 핵심: 정제된 데이터를 내 실제 MySQL DB에 적재하는 단계
    @task
    def load_to_mysql(cleaned_data):
        print("💾 우리가 만든 금고 열쇠를 이용해 MySQL DB에 직접 접속합니다.")
        # 웹 UI에서 등록했던 Connection ID(열쇠)를 적어줍니다.
        mysql_hook = MySqlHook(mysql_conn_id="my_mysql_warehouse")
        
        # 3-1. 만약 테이블이 없다면 자동으로 생성하는 마스터 SQL 쿼리 실행
        create_table_query = """
        CREATE TABLE IF NOT EXISTS mart_sales (
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
        print("✅ MySQL에 'mart_sales' 테이블이 준비되었습니다.")

        # 3-2. 정제된 데이터를 한 줄씩 DB 테이블에 INSERT (적재)
        for row in cleaned_data:
            insert_query = """
            INSERT INTO mart_sales (branch, item, price, quantity, total_price)
            VALUES (%s, %s, %s, %s, %s);
            """
            # 변수를 안전하게 바인딩하여 쿼리를 실행합니다.
            mysql_hook.run(insert_query, parameters=(row["branch"], row["item"], row["price"], row["quantity"], row["total_price"]))
            
        print(f"🎉 총 {len(cleaned_data)}건의 마트 매출 데이터가 실제 DB에 안전하게 적재되었습니다.")

    # 4. [Report] DB에 들어간 데이터를 기반으로 오늘 자 매출 요약 통계를 발행하는 단계
    @task
    def generate_summary():
        mysql_hook = MySqlHook(mysql_conn_id="my_mysql_warehouse")
        
        # DB에서 직접 오늘 총 매출액 합계를 뽑아내는 쿼리 던지기
        summary_query = "SELECT SUM(total_price) amt FROM mart_sales;"
        
        # get_first는 쿼리 결과의 첫 번째 행을 가져옵니다.
        result = mysql_hook.get_first(summary_query)
        
        total_sum = result['amt'] if result else 0
        
        print("\n📊 [오늘의 본사 통합 리포트 마감] -------------------------")
        print(f"💰 오늘 전 지점 합산 총 매출액: {total_sum}원")
        print("------------------------------------------------------------\n")

    @task
    def task99(**context):
        ti = context['ti']
        seq_id = ti.xcom_pull(task_ids="task00")
        utilCls().endLog(seq_id=seq_id , err_msg="")

    
    #raw_json = fetch_raw_data()
    cleaned_list = clean_retail_data()
    
    # 순서대로 수집 ➡️ 정제 ➡️ 적재 ➡️ 통계보고서 출력
    task00() >> cleaned_list >> load_to_mysql(cleaned_list) >> generate_summary() >> task99()
    
