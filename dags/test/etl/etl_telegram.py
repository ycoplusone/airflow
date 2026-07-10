from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.http.hooks.http import HttpHook
import json
import requests

# 💡 실습 포인트: 1단계에서 확인한 본인의 텔레그램 Chat ID 숫자를 여기에 적어주세요.
TELEGRAM_CHAT_ID = "-1002336115183"

default_args = {
    'owner': 'teacher',
    'start_date': datetime(2026, 7, 1),
}

with DAG(
    dag_id          = 'etl_telegram',
    default_args    = default_args,
    schedule        = None,  # 수동 실행 테스트용
    catchup         = False,
    tags            = ['test', 'alarm']
) as dag:

    @task
    def send_telegram_test_message():
        # 1. Airflow Connection 금고에서 telegram_conn 열쇠(토큰)를 가져옵니다.
        http_hook   = HttpHook(http_conn_id='telegramConn', method='POST')
        conn        = http_hook.get_connection('telegramConn')
        bot_token   = conn.password  # 금고에 저장한 Password(토큰) 로드
        
        # 2. 텔레그램 메시지 발송 API 주소와 데이터 조립
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        message_text = (
            "🚨 [Airflow 알림 테스트]\n"
            "과외 선생님입니다! 👨‍🏫\n"
            "현재 텔레그램 파이프라인 연동 테스트가\n"
            "아주 성공적으로 완료되었습니다. 👍"
        )
        
        payload = {
            'chat_id': TELEGRAM_CHAT_ID,
            'text': message_text
        }
        print(f"conn ==> {conn}")
        print(f"conn.extra ==>{conn.extra}")     
        json_object = json.loads(conn.extra)
        print(f"{json_object['chat_id_1']}")
        
        
        '''
        # 3. 실제 Telegram API 호출
        response = requests.post(url, json=payload)
        
        # 4. 결과 검증 (실패 시 에러를 뿜어서 Airflow 태스크를 실패 처리)
        if response.status_code == 200:
            print("🚀 텔레그램 메시지가 성공적으로 발송되었습니다!")
        else:
            raise RuntimeError(f"❌ 텔레그램 발송 실패: {response.status_code} - {response.text}")
        '''
    # 태스크 실행
    send_telegram_test_message()
    