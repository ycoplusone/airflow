from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

# 1. 비상벨 함수 정의 (동일)
def my_emergency_alert(context):
    task_id = context.get('task_instance').task_id
    print(f"\n🚨 [전역 비상벨] 태스크 [{task_id}] 가 실패하여 알림을 발송합니다!\n")

# 2. ⭐️ 핵심: 이 DAG에 속한 모든 태스크가 공유할 '기본 설정 주머니'를 만듭니다.
my_shared_settings = {
    'owner': 'dlive_2026',
    'on_failure_callback': my_emergency_alert,  # 🎯 여기에 딱 한 번만 적어주면 끝!
    #'retries': 2,                                # (보너스) 실패 시 2번까지 재시도해라!
}

# 3. DAG 선언 시 default_args 인자에 위 주머니를 통째로 넘겨줍니다.
@dag(
    dag_id="test4_callback2",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
    default_args=my_shared_settings, # ⭐️ 모든 태스크에게 상속 시키기!
)
def my_callback_pipeline():

    # 괄호 안을 텅 가볍게 비워두어도, 상속받았기 때문에 실패 시 비상벨이 울립니다!
    @task
    def dangerous_task_1():
        return 10 / 0 

    @task(trigger_rule=TriggerRule.ALL_DONE) # ⭐️ 앞 작업이 성공이든 실패든 "끝나기만 하면" 무조건 실행해라!
    def dangerous_task_2():
        return "오류" + 123 

    dangerous_task_1() >> dangerous_task_2()

my_callback_pipeline()