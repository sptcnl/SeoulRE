from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.standard.operators.python import PythonOperator
import os, json, pendulum, logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(name='MyLog')
http_hook = HttpHook(method='GET', http_conn_id='seoul_openapi')
conn = http_hook.get_connection('seoul_openapi')
extra = conn.extra_dejson
API_KEY = os.getenv('SEOUL_DATA_API_KEY')
SERVICE_NAME = "tbLnOpendataRtmsV"

def process_data(ti):
    logger.info(f'process_data 함수 들어옴')
    # API 호출 결과 JSON 받아오기
    response = ti.xcom_pull(task_ids='call_open_api')
    logger.info(f'API 호출 결과 response: {response}')
    if not response:
        print("No data received")
        return
    
    data = json.loads(response)
    items = data.get(SERVICE_NAME, {}).get('row', [])
    
    if not items:
        print("No items found")
        return
    
    # pandas DataFrame 변환 후 예시 저장
    df = pd.DataFrame(items)
    if not os.path.exists('/opt/airflow/data_files'):
        os.makedirs('/opt/airflow/data_files')
    df.to_csv('/opt/airflow/data_files/seoul_real_estate.csv', mode='a', index=False, header=False)
    print(f"{len(df)} records saved")

def task_failure_alert(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id
    task_id = task_instance.task_id if task_instance else 'unknown'
    log_url = task_instance.log_url if task_instance else 'unknown'
    response = None
    if task_instance:
        response = task_instance.xcom_pull(task_ids='call_open_api')
    logger.info(f'API 호출 결과 response: {response}')
    message = f"[Airflow Alert] DAG: {dag_id}, Task: {task_id} failed. See logs: {log_url}"
    # Log the error message or send alerts here
    logger.error(message)

with DAG(
    dag_id='seoul_real_estate_api_etl',
    start_date=pendulum.datetime(2025, 9, 27, tz="Asia/Seoul"),
    schedule='@daily',
    catchup=False,
    tags=['seoul', 'real_estate', 'api']
) as dag:

    call_open_api = HttpOperator(
        task_id='call_open_api',
        method='GET',
        http_conn_id='seoul_openapi',
        endpoint=f"{API_KEY}/json/{SERVICE_NAME}/1/1000/",
        response_filter=lambda response: response,
        log_response=True,
        on_failure_callback=task_failure_alert,
        response_check=lambda response: response.status_code == 200
    )

    process_api_data = PythonOperator(
        task_id='process_api_data',
        python_callable=process_data
    )

call_open_api >> process_api_data