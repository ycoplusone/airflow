from datetime import datetime,timedelta
import logging
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook

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

