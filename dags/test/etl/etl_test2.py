import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from lib.utilClass import utilCls       # airflow 에 사용되는 각종 유틸들.

# ==============================================================================
# 🏗️ 표준 DAG 정의부 (on_failure_callback 지정을 통해 DAG 전역 감시 카메라 가동)
# ==============================================================================
with DAG(
    dag_id                  = 'etl_test2',
    default_args            = utilCls().default_args() ,
    schedule                = None,  # 필요시 크론 표현식 주입 (예: '0 3 * * *')
    catchup                 = False,
    on_failure_callback     = utilCls().on_dag_failure_logging , # ⭐️ 여기에 등록하면 DAG 내 어떤 태스크가 깨져도 에러 로그가 남습니다!
    tags                    = ['test', 'df'],
) as dag:    

    @task
    def main(**context):
        try:
            seq_id  = utilCls().beginLog(context)     # db 에 log 시작
            data    = utilCls().getVariable() #공용변수가져오기
            # 본문 시작

            
            # 본문 종료
            utilCls().endLog(seq_id=seq_id , err_msg=str(data)) # db 에 log 종료    
        except Exception as e:            
            utilCls().errLog(seq_id=seq_id , msg=str(e))  # 오류 발생시 fail로 변경하고 오류메세지를 저장한다
        
        

    # ==============================================================================
    main()
