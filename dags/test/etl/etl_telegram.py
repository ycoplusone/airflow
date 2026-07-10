from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from lib.utilClass import utilCls
from airflow.hooks.base import BaseHook


with DAG(
    dag_id          = 'etl_telegram',
    schedule        = None,  # 수동 실행 테스트용
    catchup         = False,
    tags            = ['test', 'alarm']
) as dag:

    @task
    def send_telegram_test_message(**context):
        ''''''        
        aa = utilCls().sendTelegram(
            conn_id   = 'telegramApi',
            message_text    = '[Airflow 알림 테스트]'
        )
        print( f"result : {aa}"  )
        

    # 태스크 실행
    send_telegram_test_message()
    