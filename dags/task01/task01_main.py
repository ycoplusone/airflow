import logging
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from pprint import pformat
from airflow.exceptions import AirflowException
import traceback
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from lib.utilClass import utilCls       # airflow 에 사용되는 각종 유틸들.

with DAG(
    dag_id                  = 'task01_main' ,
    default_args            = utilCls().default_args() ,
    schedule                = None ,  # 필요시 크론 표현식 주입 (예: '0 3 * * *')
    catchup                 = False ,
    on_failure_callback     = utilCls().on_dag_failure_logging, # ⭐️ 여기에 등록하면 DAG 내 어떤 태스크가 깨져도 에러 로그가 남습니다!
    tags                    = ['jb'] ,    
) as dag:
    '''
    cafe24 db 의 가용하는 테이블 oci mysql db 로 이관하는 작업
    '''
    
    @task
    def task00(**context):
        '''start log'''
        if not context['dag_run'].conf:
            context['dag_run'].conf = {}
        context['dag_run'].conf['p_dag_id'] = 'root'
        seq_id = utilCls().beginLog(context)
        return seq_id    
    
    
    task01 = TriggerDagRunOperator(
        task_id             = "task01_trigger_etl_variable",
        trigger_dag_id      = "etl_variable",       # 🎯 깨울 기존 DAG의 정확한 dag_id
        wait_for_completion = True,              # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                      # 상태를 몇 초마다 확인할지 설정
        deferrable          = True                        # 리소스를 아끼기 위해 스마트하게 대기하는 옵션        
    )    

    task02 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_marco_info",
        trigger_dag_id      = "df_marco_info",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )    

    task03 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_barcode_info",
        trigger_dag_id      = "df_nicon_barcode_info",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )    

    task04 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_info",
        trigger_dag_id      = "df_nicon_info",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )    

    task05 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_job_list",
        trigger_dag_id      = "df_nicon_job_list",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )   

    task06 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_sale_list",
        trigger_dag_id      = "df_nicon_sale_list",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )

    task07 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_search_keywords",
        trigger_dag_id      = "df_nicon_search_keywords",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )          

    task08 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_survey_collection",
        trigger_dag_id      = "df_nicon_survey_collection",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )  

    task09 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_survey_exception_list",
        trigger_dag_id      = "df_nicon_survey_exception_list",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )  

    task10 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_survey_worst_url_list",
        trigger_dag_id      = "df_nicon_survey_worst_url_list",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )

    task11 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_nicon_target_words",
        trigger_dag_id      = "df_nicon_target_words",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    ) 

    task12 = TriggerDagRunOperator(
        task_id             = "task01_trigger_df_system_sql_log",
        trigger_dag_id      = "df_system_sql_log",          # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )           

    @task
    def task99(**context):
        '''end log'''
        ti = context['ti']
        seq_id = ti.xcom_pull(task_ids="task00")
        utilCls().endLog(seq_id=seq_id , err_msg="")

    # 직렬 처리 방법
    #task00() >> task01 >> task02 >> task03 >> task04 >> task05 >> task06 >> task07 >> task08 >> task09 >> task10 >> task11 >> task12 >> task99()

    # 병렬 처리 방법
    task00() >> task01 >> [task02,task03,task04,task05,task06,task07,task08,task09,task10,task11,task12] >>task99()
    