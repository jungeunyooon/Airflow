import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def fetch_korean_advice():
    url = "https://korean-advice-open-api.vercel.app/api/advice"
    try:
        response = requests.get(url)
        response.raise_for_status()  # 응답 코드가 200이 아니면 예외 발생
        # API가 {"author": "...", "authorProfile": "...", "message": "..."} 형태로 반환된다고 가정
        data = response.json()
        advice = data.get("message", "명언을 받지 못했습니다.")
        author = data.get("author", "알 수 없음")
        author_profile = data.get("authorProfile", "")
    except Exception as e:
        advice = f"명언을 가져오는 데 실패했습니다: {e}"
        author = ""
        author_profile = ""
    
    # 현재 날짜와 시간을 문자열로 변환
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    output_message = f"날짜: {current_time} | 오늘의 명언: {advice}"
    if author:
        output_message += f" | 작성자: {author} ({author_profile})"
    
    # 여기서는 단순히 출력하지만, 이메일 발송 등 다른 동작으로 대체할 수 있음
    print(output_message)
    
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),  # 필요에 따라 고정 날짜로 설정 가능
}

with DAG(
    dag_id='weekday_morning_advice',
    default_args=default_args,
    schedule_interval="0 7 * * 1-5",  # 평일(월~금) 오전 7시 실행 (Cron: 분 시 일 월 요일)
    catchup=False,
    tags=['korean', 'advice'],
) as dag:
    
    fetch_advice_task = PythonOperator(
        task_id='fetch_korean_advice',
        python_callable=fetch_korean_advice
    )
