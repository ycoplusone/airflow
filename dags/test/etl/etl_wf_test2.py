from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from lib.utilClass import utilCls       # airflow 에 사용되는 각종 유틸들.

with DAG(
    dag_id                  = 'etl_wf_test2',
    default_args            = utilCls().default_args() ,
    schedule                = None,  # 필요시 크론 표현식 주입 (예: '0 3 * * *')
    catchup                 = False,
    on_failure_callback     = utilCls().on_dag_failure_logging , # ⭐️ 여기에 등록하면 DAG 내 어떤 태스크가 깨져도 에러 로그가 남습니다!
    tags                    = ['test', 'wf'],
) as dag:  
    
    @task
    def task00(**context):
        seq_id = utilCls().beginLog(context)
        return seq_id
        
    # 1. 첫 번째 기존 DAG(etl_bulk_load) 호출 태스크
    task01 = TriggerDagRunOperator(
        task_id             = "trigger_etl_wf_test2_1",
        trigger_dag_id      = "etl_test2",       # 🎯 깨울 기존 DAG의 정확한 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,              # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                      # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,          # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )

    # 2. 두 번째 기존 DAG(etl_test) 호출 태스크
    task02 = TriggerDagRunOperator(
        task_id             = "trigger_etl_wf_test2_2",
        trigger_dag_id      = "etl_test3",             # 🎯 또 다른 기존 DAG의 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )

    # 3. 두 번째 기존 DAG(etl_test) 호출 태스크
    task03 = TriggerDagRunOperator(
        task_id             = "trigger_etl_wf_test2_3",
        trigger_dag_id      = "etl_test4_bulk",             # 🎯 또 다른 기존 DAG의 dag_id
        conf                = {"p_dag_id": "{{ dag.dag_id }}"},
        wait_for_completion = True,               # ⭐ 필수! 이 DAG가 '완전히 끝날 때까지' 기다렸다가 다음으로 넘어감 (직렬 핵심)
        poke_interval       = 10,                       # 상태를 몇 초마다 확인할지 설정
        deferrable          = True,                        # ⭐️ 중요: 대기하는 동안 Worker 슬롯을 반납하여 데드락 방지!
    )    

    @task
    def task99(**context):
        ti = context['ti']
        seq_id = ti.xcom_pull(task_ids="task00")
        utilCls().endLog(seq_id=seq_id , err_msg="")

    # ⛓️ 철길 깔기 (직렬 연결)
    task00() >> task01 >> task02 >> task03 >> task99()
