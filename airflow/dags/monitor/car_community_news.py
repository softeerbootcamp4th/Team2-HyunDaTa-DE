import os
import json
import yaml
import boto3
import pendulum
import requests
from datetime import datetime, timedelta
from xml.etree import ElementTree as ET
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


local_tz = pendulum.timezone('Asia/Seoul')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 9, 9, 0, 0, tzinfo=local_tz),
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

        with open(f'/tmp/{save_filename}', 'w') as f:
            json.dump(recent_articles, f)


################# define tasks #################
autoview_task = PythonOperator(
    task_id='autoview_news_monitor',
    python_callable=fetch_recent_news,
    provide_context=True,
    op_kwargs={'comm_name': 'autoview'},
    dag=dag,
)

motorgraph_task = PythonOperator(
    task_id='motorgraph_news_monitor',
    python_callable=fetch_recent_news,
    provide_context=True,
    op_kwargs={'comm_name': 'motorgraph'},
    dag=dag,
)

auto_electronics_task = PythonOperator(
    task_id='autoelectronics_news_monitor',
    python_callable=fetch_recent_news,
    provide_context=True,
    op_kwargs={'comm_name': 'autoelectronics'},
    dag=dag,
)

gp_korea_task = PythonOperator(
    task_id='gp_korea_news_monitor',
    python_callable=fetch_recent_news,
    op_kwargs={'comm_name': 'gpkorea'},
    provide_context=True,
    dag=dag,
)

autoherald_task = PythonOperator(
    task_id='autoherald_news_monitor',
    python_callable=fetch_recent_news,
    op_kwargs={'comm_name': 'autoherald'},
    provide_context=True,
    dag=dag,
)

carguy_task = PythonOperator(
    task_id='carguy_news_monitor',
    python_callable=fetch_recent_news,
    provide_context=True,
    op_kwargs={'comm_name': 'carguy'},
    dag=dag,
)

motoya_task = PythonOperator(
    task_id='motoya_news_monitor',
    python_callable=fetch_recent_news,
    provide_context=True,
    op_kwargs={'comm_name': 'motoya'},
    dag=dag,
)

autotimes_task = PythonOperator(
    task_id='autotimes_news_monitor',
    python_callable=fetch_recent_news,
    provide_context=True,
    op_kwargs={'comm_name': 'autotimes'},
    dag=dag,
)


autoview_task
motorgraph_task
auto_electronics_task
gp_korea_task
autoherald_task
carguy_task
motoya_task
autotimes_task
