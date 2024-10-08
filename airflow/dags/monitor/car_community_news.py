import os
import json
import yaml
import pendulum
import requests
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'car_community_news_monitor',
    default_args=default_args,
    description='car community news monitoring every hour',
    tags=['car_community_news', 'monitoring'],
    schedule_interval='0 * * * *',  # 매 시간 0분에 실행
)


def convert_to_datetime(comm_name: str, pub_date_str: str):
    if comm_name in ['autoview', 'motorgraph', 'autoherald', 'autotimes']:
        pub_date = datetime.strptime(
            pub_date_str, "%a, %d %b %Y %H:%M:%S %z")
    elif comm_name == 'autoelectronics':
        pub_date = datetime.strptime(
            pub_date_str, '%Y-%m-%d %H:%M:%S%z')
    elif comm_name in ['gpkorea', 'carguy', 'motoya']:
        pub_date = datetime.strptime(
            pub_date_str, "%Y-%m-%d %H:%M:%S")
    return pub_date


def fetch_recent_news(**kwargs):
    # 서울 시간대로 변환
    comm_name = kwargs['comm_name']
    execution_date = kwargs['execution_date']

    # RSS 피드 URL
    with open('config/car_community_news.yaml', 'r') as f:
        config = yaml.safe_load(f)

    comm_url = config['community_sites'][comm_name]
    response = requests.get(comm_url)
    root = ET.fromstring(response.content)

    seoul_time, time_diff = timedelta(hours=9), timedelta(hours=1)
    recent_articles = []

    execution_date = pendulum.instance(execution_date)
    seoul_now = execution_date + seoul_time

    for item in root.findall('.//item'):
        pub_date = convert_to_datetime(comm_name, item.find('pubDate').text)
        pub_date = pendulum.instance(pub_date)

        if seoul_now - time_diff <= pub_date <= seoul_now:
            article = {
                'title': item.find('title').text,
                'link': item.find('link').text,
                'pubDate': pub_date.strftime('%Y-%m-%d %H:%M:%S'),
                'description': item.find('description').text
            }
            recent_articles.append(article)

    if len(recent_articles) > 0:
        cur_event_time = seoul_now.strftime('%Y-%m-%d-%H')
        save_filename = f'{cur_event_time}_{comm_name}_recent_articles.json'

        kwargs['task_instance'].xcom_push(
            key='key', value=save_filename)  # S3에 저장할 파일명
        kwargs['task_instance'].xcom_push(
            key='file', value=recent_articles)

        return [save_filename, recent_articles]


def check_branch(**kwargs):
    task_id = kwargs['task_id']
    upload_s3_task_id = kwargs['upload_task_id']
    save_filename = kwargs['ti'].xcom_pull(task_ids=task_id)
    if save_filename is None:
        return 'no_search_post'
    else:
        return upload_s3_task_id


def no_search_post(**kwargs):
    print('No recent articles found. Exiting task...')


def upload_file_to_s3(**kwargs):
    load_dotenv("config/aws.env")
    bucket_name = os.getenv('BUCKET_NAME')
    task_id = kwargs['task_id']
    xcom_value = kwargs['task_instance'].xcom_pull(
        task_ids=task_id)

    key, file = xcom_value[0], xcom_value[1]
    key_name = f"monitor/news/{key}"
    with open(f"/tmp/{key}", 'w') as f:
        json.dump(file, f, ensure_ascii=False)

    s3 = S3Hook(aws_conn_id='aws_s3_conn')  # Airflow에 설정한 AWS 연결 ID 사용
    s3.load_file(
        filename=f"/tmp/{key}",
        key=key_name,
        bucket_name=bucket_name,
        replace=True
    )
    print(
        f"File {f'/tmp/{key}'} has been uploaded to S3 bucket {bucket_name} with key {key_name}.")
    os.remove(f"/tmp/{key}")


no_search_post_task = PythonOperator(
    task_id=f'no_search_post',
    python_callable=no_search_post,
    provide_context=True,
    dag=dag,
)


def create_news_monitoring_tasks(comm_name, dag):
    monitor_task = PythonOperator(
        task_id=f'{comm_name}_news_monitor',
        python_callable=fetch_recent_news,
        provide_context=True,
        op_kwargs={'comm_name': comm_name},
        dag=dag,
    )

    upload_task = PythonOperator(
        task_id=f'{comm_name}_upload_file_to_s3',
        python_callable=upload_file_to_s3,
        provide_context=True,
        op_kwargs={'task_id': f'{comm_name}_news_monitor'},
        dag=dag,
    )

    branch_task = BranchPythonOperator(
        task_id=f'{comm_name}_branch_task',
        python_callable=check_branch,
        provide_context=True,
        op_kwargs={
            'task_id': f'{comm_name}_news_monitor',
            'upload_task_id': f'{comm_name}_upload_file_to_s3'
        },
        dag=dag,
    )

    monitor_task >> branch_task >> [no_search_post_task, upload_task]


communities = ['autoview', 'motorgraph', 'autoelectronics',
               'gpkorea', 'autoherald', 'carguy', 'motoya', 'autotimes']

for community in communities:
    create_news_monitoring_tasks(community, dag)
