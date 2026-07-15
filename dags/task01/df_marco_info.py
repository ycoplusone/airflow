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
    dag_id                  = 'df_marco_info',
    default_args            = utilCls().default_args() ,
    schedule                = None,  # 필요시 크론 표현식 주입 (예: '0 3 * * *')
    catchup                 = False,
    tags                    = ['test', 'df'],
) as dag:
    # 1. [Extract & Load] 원본 DB 테이블에서 데이터를 읽어와 대상 DB 테이블로 적재하는 태스크
    @task
    def main(**context):
        try:
            seq_id  = utilCls().beginLog(context)     # db 에 log 시작        
            data    = utilCls().getVariable() #공용변수가져오기

            """
            MySQL Table to MySQL Table 데이터 이관을 수행합니다.
            - E(Extract): 원본 DB에서 데이터를 조회합니다.
            - L(Load): 대상 DB에 데이터를 적재합니다. (TRUNCATE-INSERT 방식)
            """
            # ======================================================================
            # 1. 설정 (사용자 환경에 맞게 수정)
            # ======================================================================
            # Airflow UI의 Admin > Connections 에서 등록한 Connection ID
            source_conn_id = "mysql_cafe24"         # 👈 데이터를 읽어올 원본 DB Connection
            target_conn_id = "mysql_devDB"          # 👈 데이터를 적재할 대상 DB Connection

            source_table = "marco_info"    # 👈 원본 테이블명
            target_table = "marco_info"    # 👈 대상 테이블명

            # 대상 테이블에 적재할 컬럼 목록 (순서 중요)
            target_columns = ["seq","job_nm","job_st_dt","job_ed_dt","job_st_cnt","job_ed_cnt","url"]

            # ======================================================================
            # 2. Extract: 원본 테이블에서 데이터 추출
            # ======================================================================
            print(f"📖 [Extract] 원본 DB '{source_conn_id}'의 '{source_table}' 테이블에서 데이터 조회를 시작합니다.")
            source_hook = MySqlHook(mysql_conn_id=source_conn_id)
            # get_records: 결과를 튜플의 리스트 형태로 반환합니다. -> [(val1, val2), (val1, val2), ...]
            # 이 형태는 insert_rows 메서드에 바로 사용하기에 최적입니다.
            records = source_hook.get_records(f"SELECT {', '.join(target_columns)} FROM {source_table}")
            if not records:
                print("⚠️ 원본 데이터가 없습니다. 작업을 중단합니다.")
                return
            print(f"✅ 총 {len(records)}건의 데이터를 성공적으로 추출했습니다.")

            # ======================================================================
            # 3. Load: 대상 테이블에 데이터 적재
            # ======================================================================
            print(f"💾 [Load] 대상 DB '{target_conn_id}'의 '{target_table}' 테이블에 데이터 적재를 시작합니다.")
            target_hook = MySqlHook(mysql_conn_id=target_conn_id)
            
            # 3-1. (초기화) 기존 데이터 삭제 (Full Refresh)
            print(f"🗑️  기존 '{target_table}' 테이블의 데이터를 모두 삭제합니다. (TRUNCATE)")
            target_hook.run(f"TRUNCATE TABLE {target_table}")

            # 3-2. (적재) Bulk Insert 수행
            print(f"🚀 추출한 {len(records)}건의 데이터를 Bulk Insert로 고속 적재합니다.")
            target_hook.insert_rows(table=target_table, rows=records, target_fields=target_columns)
            print(f"🎉 총 {len(records)}건의 데이터가 '{target_table}' 테이블에 성공적으로 적재되었습니다.")

            utilCls().endLog(seq_id=seq_id , err_msg=str(data)) # db 에 log 종료    
        except Exception as e:
            utilCls().errLog(seq_id=seq_id , msg=str(e))  # 오류 발생시 fail로 변경하고 오류메세지를 저장한다

     
    # DAG 실행
    main() 
    
