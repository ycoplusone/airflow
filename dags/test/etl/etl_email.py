from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from lib.utilClass import utilCls


with DAG(
    dag_id='etl_email',
    schedule=None,
    catchup=False,    
) as dag:

    @task
    def send_email(**context):
        utilCls().sendEmail(
            connection_id='smtpGmail',
            subject='111[Airflow 우회성공] 파이썬 엔진으로 발송된 알림 메일입니다 🚀',
            html_content="""<h3>안녕하세요, 학생분! 과외 선생님입니다. �👨‍🏫</h3> """, 
            receiver="ycoplusone@dlive.kr",
            file_path="/home/dlive/text.txt"            
        )   

    @task
    def send_email2(**context):
        utilCls().sendEmail(
            connection_id='smtpGmail',
            subject='123[Airflow 우회성공] 파이썬 엔진으로 발송된 알림 메일입니다 🚀',
            html_content="""<h3>안녕하세요, 학생분! 과외 선생님입니다. �👨‍🏫</h3> """, 
            receiver="ycoplusone@dlive.kr",
        )           

    send_email() >> send_email2()


