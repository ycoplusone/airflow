from datetime import datetime,timedelta
import logging
import os
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import requests

import pymysql
pymysql.install_as_MySQLdb()

class utilCls():

    __MYSQL_CONN_ID = "my_mysql_warehouse"

    def __init__(self):
        pass

    def get_current_seoul_time(self):
        """한국 시간(KST)으로 포맷팅된 현재 시간을 반환합니다."""
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    def default_args(self):
        default_args = {
            'owner': 'airflow',
            'start_date': datetime(2026, 7, 1),
            'retries': 1,                           # 이상 발생 시 최대 3회 재시도
            'retry_delay': timedelta(seconds=10),    # 재시도 사이의 대기 시간 (5분)
        }
        return default_args
    
    def on_dag_failure_logging(self , context):
        """
        DAG 중간에 어떤 태스크든 실패하면 백그라운드에서 이 함수가 강제 소환됩니다.
        """
        dag_id          = context['dag'].dag_id
        task_instance   = context['task_instance']

        seoul_now = self.get_current_seoul_time()    
        
        logging.info(f"🚨 [standard_etl_template_v1] An error was detected. Logging the error to the DB.")
        
        mysql_hook = MySqlHook(mysql_conn_id = self.__MYSQL_CONN_ID)
        
        # 특정 시퀀스(seq_id)를 모를 경우를 대비하여, 당일 실패한 dag_id 기준으로
        # 가장 최근에 'RUNNING' 상태였던 레코드를 찾아 에러 상태로 업데이트합니다.
        insert_sql = """
            INSERT INTO dag_execution_logs (dag_id, start_time, status,created_at,error_message)
            VALUES (%s, %s, 'FAILED',%s,'????')
        """
        
        try:
            mysql_hook.run(insert_sql, parameters=(dag_id, seoul_now , seoul_now))
            #logging.info("✅ Successfully logged the error to the DB.")
        except Exception as e:
            logging.error(f"❌ FAILED to update the DB during the callback: {str(e)}")    

    def beginLog(self , context ):
        '''
        💾 [시작 로깅] DB 적재 완료 - 발급된 시퀀스 번호: {seq_id}
        '''
        dag_id      = context['dag'].dag_id
        seoul_now   = self.get_current_seoul_time()
        dag_run     = context['dag_run']
        p_dag_id    = dag_run.conf.get('p_dag_id') if dag_run.conf else 'single'


        mysql_hook = MySqlHook(mysql_conn_id=self.__MYSQL_CONN_ID)
        
        insert_sql = """
            INSERT INTO dag_execution_logs (dag_id, start_time, status,created_at,p_dag_id)
            VALUES (%s, %s, 'RUNNING',%s,%s)
        """
        # 데이터 적재 실행
        mysql_hook.run(insert_sql, parameters=(dag_id, seoul_now , seoul_now,p_dag_id))
        
        # 방금 인서트된 오토인크리먼트 시퀀스 ID(seq_id)를 가져옵니다.
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT max(seq_id) seq_id from dag_execution_logs where dag_id ='{dag_id}'; ")
        result = cursor.fetchone()
        seq_id = result.get('seq_id',0) if isinstance(result, dict) else result[0]        

        return seq_id
    
    def errLog(self , seq_id:str = '' , msg:str = ''):
        """
        DAG 중간에 어떤 태스크든 실패하면 백그라운드에서 이 함수가 강제 소환됩니다.
        """
        mysql_hook = MySqlHook(mysql_conn_id = self.__MYSQL_CONN_ID)
        db_error_msg = f"{msg}"        
        seoul_now = self.get_current_seoul_time()                            

        # 가장 최근에 'RUNNING' 상태였던 레코드를 찾아 에러 상태로 업데이트합니다.
        error_sql = """
            UPDATE dag_execution_logs
            SET end_time = %s, status = 'FAILED', error_message = %s
            WHERE seq_id = %s AND status = 'RUNNING'
            ORDER BY seq_id DESC LIMIT 1
        """        
        try:
            mysql_hook.run(error_sql, parameters=(seoul_now, db_error_msg, seq_id))
            logging.info("✅ Successfully logged the error to the DB.")
        except Exception as e:
            logging.error(f"❌ FAILED to update the DB during the callback: {str(e)}")      

    def endLog(self , seq_id:str='' , err_msg:str=''):
        '''🏁 [완료 로깅] 시퀀스 번호 [{seq_id}] 파이프라인 마감 성공 처리 완료!        '''
        seoul_now = self.get_current_seoul_time()        
        
        mysql_hook = MySqlHook(mysql_conn_id=self.__MYSQL_CONN_ID)
        
        update_sql = """
            UPDATE dag_execution_logs
            SET end_time = %s, status = 'SUCCESS' , error_message = %s
            WHERE seq_id = %s
        """
        mysql_hook.run(update_sql, parameters=(seoul_now, err_msg ,seq_id))

    def getVariable(self):        
        '''🎯 데이터를 성공적으로 가져왔습니다!!!'''        
        result = Variable.get(key="variable")
        return result

    def sendEmail(self, connection_id:str = 'smtpGmail', subject:str = '', html_content:str = '', receiver:str = '',file_path:str=''):
        """
        Airflow의 SMTP Hook을 사용하여 이메일을 발송합니다.
        """
        from email.mime.text import MIMEText
        from email.mime.base import MIMEBase
        from email.mime.multipart import MIMEMultipart
        import smtplib
        from email import encoders

        # 1. Airflow의 SMTP Hook을 사용하여 연결 설정
        base_hook = BaseHook.get_connection(connection_id)

        # 3. 이메일 발송
        # 1. 처음에 완벽히 성공했던 순수 파이썬 smtplib 통신망 개설
        smtp_host   = base_hook.host
        smtp_port   = base_hook.port
        sender      = base_hook.login  # SMTP 연결에서 가져온 발신자 이메일 주소        
        password    = base_hook.password  # SMTP 연결에서 가져온 비밀번호        
        
        
        msg             = MIMEMultipart()
        msg['Subject']  = subject
        msg['From']     = base_hook.login  # SMTP 연결에서 가져온 발신자 이메일 주소
        msg['To']       = receiver #수신자
        msg.attach(MIMEText(html_content, 'html', 'utf-8'))

        
        if os.path.exists(file_path) and file_path != '' :
            # 파일을 바이너리 읽기(rb) 모드로 엽니다.
            with open(file_path, 'rb') as f:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(f.read())
                
            # 바이너리 데이터를 이메일 전송에 적합하게 인코딩(Base64)합니다.
            encoders.encode_base64(part)
            
            # 파일명을 헤더에 예쁘게 얹어줍니다.
            filename = os.path.basename(file_path)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename="{filename}"'
            )
            
            # 메일 본체에 첨부파일을 찰떡같이 결합합니다.
            msg.attach(part)        
        
        # 3. 발송        
        server = smtplib.SMTP(smtp_host, smtp_port)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, [receiver], msg.as_string())
        server.quit()        

    
    def sendTelegram(self , conn_id:str='telegramApi', message_text:str = ''):
        '''텔레그램 발송'''

        # 1. Airflow Connection 금고에서 telegram_conn 열쇠(토큰)를 가져옵니다.
        base_hook           = BaseHook.get_connection(conn_id)  # Airflow Connection 금고에서 telegram_conn 열쇠(토큰)를 가져옵니다.
        bot_token           = base_hook.password  # 금고에 저장한 Password(토큰) 로드
        chat_id             = base_hook.extra_dejson.get('chat_id')  # 금고에 저장한 Extra(chat_id) 로드
        
        # 2. 텔레그램 메시지 발송 API 주소와 데이터 조립
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"        
        
        payload = {
            'chat_id': chat_id,
            'text': message_text
        }        

        # 3. 실제 Telegram API 호출
        res = requests.post(url, json=payload)
        
        # 4. 결과 검증 (실패 시 에러를 뿜어서 Airflow 태스크를 실패 처리)
        return res.status_code
