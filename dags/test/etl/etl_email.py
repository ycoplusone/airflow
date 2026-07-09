from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime


'''
메일 발송 테스트는 개같이 안되네 안해.
징징징징

'''
with DAG(dag_id='etl_email', start_date=datetime(2026, 1, 1)) as dag:
    send_email = EmailOperator(
        task_id='send_notification',
        to='ycoplusone@gmail.com',
        subject='Airflow Test Email',
        html_content='<p>This is a test email from Airflow.</p>'
    )   
