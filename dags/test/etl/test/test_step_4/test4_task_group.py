from datetime import datetime
from airflow.decorators import dag, task, task_group # ⭐️ task_group 데코레이터를 추가로 가져옵니다.

@dag(
    dag_id="test4_task_group",
    start_date=datetime(2026, 6, 1),
    schedule=None,
    catchup=False,
)
def my_task_group_pipeline():

    @task
    def start_project():
        print("🚀 마트 데이터 통합 분석 프로젝트를 시작합니다.")

    # 🏢 1번 그룹: 서울 지점 데이터 처리 모듈
    @task_group(group_id="seoul_branch_group")
    def seoul_process():
        @task
        def collect_data():
            return "서울 지점 매출 데이터 수집 완료"

        @task
        def clean_data(raw_data):
            print(f"🧹 {raw_data}를 깨끗하게 정제합니다.")
            return "서울 정제 데이터"

        # 그룹 내부의 흐름을 정의합니다.
        raw = collect_data()
        return clean_data(raw)

    # 🏢 2번 그룹: 부산 지점 데이터 처리 모듈
    @task_group(group_id="busan_branch_group")
    def busan_process():
        @task
        def collect_data():
            return "부산 지점 매출 데이터 수집 완료"

        @task
        def clean_data(raw_data):
            print(f"🧹 {raw_data}를 깨끗하게 정제합니다.")
            return "부산 정제 데이터"

        # 그룹 내부의 흐름을 정의합니다.
        raw = collect_data()
        return clean_data(raw)

    @task
    def finalize_report(seoul_res, busan_res):
        print(f"🏁 [최종 완료] {seoul_res}와 {busan_res}를 합쳐 본사 통합 리포트를 발행합니다!")

    # 뼈대 파이프라인 연결 구조 설계
    start = start_project()
    
    # 각 지점 그룹 함수를 호출하여 인스턴스를 만듭니다.
    seoul_result = seoul_process()
    busan_result = busan_process()
    
    # 프로젝트 시작 -> 두 지점 병렬 그룹 가동 -> 최종 리포트 마감
    start >> [seoul_result, busan_result] >> finalize_report(seoul_result, busan_result)

my_task_group_pipeline()