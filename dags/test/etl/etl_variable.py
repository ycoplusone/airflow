from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.decorators import task
import logging
from airflow.models import Variable

with DAG(
    dag_id                  = "etl_variable",
    start_date              = datetime(2026, 7, 1),
    schedule                = None,
    catchup                 = False,
    tags                    = ['test', 'variable'],
) as dag:

    # [저장하는 태스크]
    @task
    def setVariable(**context):
        ti   = context['task_instance']
        # 1. 오늘 기준 datetime 객체 생성
        today_dt = datetime.now()

        # 2. 오늘 / 어제 계산
        today = today_dt.date()
        yesterday = today - timedelta(days=1)

        # 3. 당월 / 전월 계산 (연, 월만 추출)
        this_month = today_dt.strftime('%Y%m')

        # relativedelta를 사용하면 1월에서 1달을 뺐을 때 작년 12월로 안전하게 계산됩니다.
        last_month_dt = today_dt - relativedelta(months=1)
        last_month = last_month_dt.strftime('%Y%m')
        result_data = {"this_dt":today.strftime('%Y%m%d'),
            "pre_dt":yesterday.strftime('%Y%m%d'),
            "this_mm":this_month,
            "pre_mm":last_month
        }
        Variable.set(key="variable", value=result_data, serialize_json=True)    

    setVariable()
    