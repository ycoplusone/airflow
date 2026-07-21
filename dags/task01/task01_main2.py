from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.models import DagModel
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.session import provide_session

# 트리거하고 싶은 타겟 태그 설정
TARGET_TAG = "task01"

with DAG(
    dag_id="task01_main2",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 수동 실행(Trigger)을 위해 None 설정
    catchup=False,
    tags=["Management", "Trigger-Utility"],
    description="특정 태그를 가진 DAG들을 자동으로 찾아 일괄 실행합니다."
) as dag:

    # 1. 메타데이터 DB에서 특정 태그를 가진 활성화된 DAG ID들을 조회하는 태스크
    @task
    @provide_session
    def get_dag_ids_by_tag(tag_name: str, session=None) -> list:
        # DB에서 태그 매칭 + 활성화(Active) + 일시정지 상태가 아님(Not Paused) 필터링
        matching_dags = (
            session.query(DagModel)
            .filter(DagModel.tags.any(name=tag_name))
            .filter(DagModel.is_active == True)
            .filter(DagModel.is_paused == False)
            # 현재 이 유틸리티 DAG 자체는 제외 (무한 루프 방지)
            .filter(DagModel.dag_id != "trigger_dags_by_tag")
            .all()
        )
        
        dag_ids = [d.dag_id for d in matching_dags]
        print(f"🎯 발견된 '{tag_name}' 태그 DAG 목록: {dag_ids}")
        return dag_ids

    # 2. 동적 태스크 생성 (Dynamic Task Mapping)
    # 발견된 DAG 리스트의 개수만큼 TriggerDagRunOperator를 실시간으로 복제하여 병렬 수행합니다.
    target_dag_list = get_dag_ids_by_tag(TARGET_TAG)

    trigger_tasks = TriggerDagRunOperator.partial(
        task_id="trigger_target_dag",
        wait_for_completion=False,     # True로 바꾸면 대상 DAG들이 모두 끝날 때까지 기다립니다.
        deferrable=True,               # 대기할 때 Worker 슬롯을 반납하여 시스템 데드락을 방지합니다.
    ).expand(
        trigger_dag_id=target_dag_list # 동적 매핑으로 리스트 안의 dag_id를 각각 매핑합니다.
    )

    # 파이프라인 흐름 정의
    target_dag_list >> trigger_tasks