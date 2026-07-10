from datetime import datetime
from airflow.decorators import dag, task

@dag(
    dag_id="test4_dynamic",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)
def my_dynamic_pipeline():

    # 1. 오늘 처리해야 할 타겟 지점 리스트를 유동적으로 반환하는 태스크
    # 실무에서는 이 부분이 "DB에서 오늘 활성화된 지점 목록 가져오기"가 됩니다.
    @task
    def get_active_branches():
        # 일부러 3개의 지점을 리스트로 던져보겠습니다.
        branches = ["Seoul", "Busan", "Jeju","for","get","me","let me go"]
        print(f"🏢 오늘 가동하는 지점 목록: {branches}")
        return branches

    # 2. ⭐️ 오늘의 주인공: 동적으로 복사될 태스크
    # 일반 태스크처럼 만들지만, 아래에서 호출할 때 마법이 일어납니다.
    @task
    def process_branch_data(branch_name):
        print(f"🚀 [{branch_name}] 지점의 대용량 매출 데이터를 병렬 처리 중입니다!")
        return f"{branch_name} 처리 완료"

    # 3. 동적으로 생성된 모든 태스크의 결과를 모아서 최종 마감하는 태스크
    @task
    def summary_report(results):
        print(f"🏁 [최종 리포트 마감] 모든 지점 결과 취합 완료: {results}")

    # 뼈대 연동 및 동적 매핑(Mapping) 처리
    branch_list = get_active_branches()
    
    # ⭐️ 핵심: 함수명 뒤에 () 대신 .expand(매개변수=리스트)를 붙여줍니다!
    # Airflow가 리스트의 개수(3개)를 보고 이 태스크를 3개로 자동 복사해서 병렬 구동합니다.
    mapped_tasks = process_branch_data.expand(branch_name=branch_list)
    
    # 최종 마감방으로 연결
    summary_report(mapped_tasks)

my_dynamic_pipeline()