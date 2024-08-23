import os
import re
import json
import boto3
import pandas as pd
import pymysql
from dotenv import load_dotenv
from openai import OpenAI

# rds.env 파일의 경로를 지정하여 환경 변수 불러오기
load_dotenv('.env')
# 환경 변수에서 RDS 정보 불러오기
rds_host = os.getenv('RDS_HOST')
rds_user = os.getenv('RDS_USER')
rds_password = os.getenv('RDS_PASSWORD')
rds_database = os.getenv('RDS_DATABASE')
buekct_name = os.getenv('BUCKET_NAME')
api_key=os.getenv('API_KEY')
s3 = boto3.client('s3')


total = [0]
cur = [0]


# S3에서 CSV 파일들을 읽어 DataFrame으로 반환하는 함수
def read_csvs_in_s3(newses: list, car_names: list, start_datetime, end_datetime) -> pd.DataFrame:
    all_data = []
    for news in newses:
        for car_name in car_names:
            s3_key = f"monitor/news/{news}/{start_datetime.strftime('%Y%m%d')}/{start_datetime.strftime('%Y%m%d_%H%M')}_{end_datetime.strftime('%Y%m%d_%H%M')}_{news}_{car_name}.csv"
            try:
                obj = s3.get_object(Bucket=buekct_name, Key=s3_key)
                df = pd.read_csv(obj['Body'])
                all_data.append(df)
            except Exception as e:
                print(f"Error reading {s3_key}: {e}")
    if not all_data:
        return pd.DataFrame()
    combined_df = pd.concat(all_data)
    return combined_df
def extract_features_by_prompting(text, client):
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "user",
                "content": f"""
                [뉴스 제목 및 본문]
                {text}

                [할 것]
                - 위의 뉴스 내용을 기반으로 기반으로 아래 작업을 수행하라.
                - 답변은 반드시 한글로 작성되어야 한다.
                - 해당 뉴스에서 다음의 네 가지를 추출하라.
                >> 차종: 뉴스의 내용과 가장 관련이 있는 자동차 모델명 한 개.
                >> 이슈 여부 및 주제: 결함/이슈와 관련된 내용이라면 [issue="True", topic="주제"]를 반환. 결함/이슈와 관련된 내용이 아니라면 [issue="False", topic="None"]를 반환.
                >> 요약: 본문을 3문장으로 요약

                - 단, topic은 다음 리스트 중 하나를 선택하여야 함. 관련 주제가 리스트에 존재하지 않는다면 "None"을 출력
                - ["ICCU", "가속", "급발진", "냉각수누수", "녹/부식", "누수", "누유", "단차", "도어잠금", "떨림", "방전", "배기", "배터리", "변속기", "브레이크패드", "서스펜션", "소프트웨어", "스티어링", "시동", "안전벨트", "에어백", "에어컨", "엔진", "엔진소음", "연료누출", "와이퍼", "전기", "전방 센서", "제동", "조명", "차선이탈경고시스템", "코팅", "타이어", "트렁크", "핸들", "후방카메라", "히터"]

                - 결과는 아래 format을 따라서 JSON으로 반환.
                    {{
                        "car_name": "[차종 / None]",
                        "issue": "[True / False]",
                        "topic": "[주제 / None]"
                        "summary": "[본문 요약 결과]",
                    }}
                """
            }
        ],
        temperature=0.2,
    )

    try:
        raw_output = completion.choices[0].message.content.strip()
        left = raw_output.find('{')
        right = raw_output.rfind('}')
        summary_content = raw_output[left:right+1]
        summary_json = json.loads(summary_content)
            
        car_name = summary_json.get('car_name', None)
        issue = summary_json.get('issue', False) == "True"
        topic = summary_json.get('topic', None)
        summary = summary_json.get('summary', '')

        print(raw_output)
        print(f"{car_name} | {issue} | {topic}\n{summary}\n\n")
        cur[0] += 1
        print(cur, '/', total)
        return car_name, issue, topic, summary
    except Exception as e:
        print("[ERROR]", e)
        return None, False, None, None 


# AWS RDS에 연결하는 함수
def connect_to_rds():
    try:
        connection = pymysql.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            database=rds_database,
            connect_timeout=5
        )
        print("Successfully connected to AWS RDS.")
        return connection
    except pymysql.MySQLError as e:
        print(f"ERROR in connecting to RDS: {e}")
        return None
    

# RDS의 news 테이블을 업데이트하는 함수
def update_rds_table(df):
    connection = connect_to_rds()
    insert_update_query = """
        INSERT IGNORE INTO news (upload_date, upload_time, title, car_name, issue, summary, url, is_trigger)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    data_to_insert = [
        (
            row['upload_date'],
            row['upload_time'],
            row['title'],
            row['car_name'],
            row['topic'],
            row['summary'],
            row['url'],
            row['is_trigger']
        )
        for _, row in df.iterrows()
    ]
    with connection.cursor() as cursor:
        cursor.executemany(insert_update_query, data_to_insert)
    connection.commit()
    connection.close()

def lambda_handler(event, context):
    newses = event.get('newses')
    car_names = event.get('car_names')
    time_ranges = event.get('time_ranges')
    start_datetime = pd.to_datetime(min(tr['start_datetime'] for tr in time_ranges))
    end_datetime = pd.to_datetime(max(tr['end_datetime'] for tr in time_ranges))

    df = read_csvs_in_s3(newses, car_names, start_datetime, end_datetime)
    print("Number of crawled newses:", df.shape[0])
    total[0] = len(df)

    if df.empty:
        print("No crawled data")
        return {
            'statusCode': 200,
            'body': 'RDS news table was not updated, but it ran successfully'
        }

    client = OpenAI(api_key=api_key)
    df = df.fillna('')
    temp_df = (df['title'] + '\n' + df['body']).apply(extract_features_by_prompting, args=(client,))
    df['car_name'], df['is_trigger'], df['topic'], df['summary'] = zip(*temp_df)
    update_rds_table(df[(df['is_trigger'].lower() != "none") & (df['topic'].str.lower() != "none")])



    return {
        'statusCode': 200,
        'body': 'RDS news table updated successfully'
    }

if __name__ == "__main__":
    event = dict(
        newses = ['kbs', 'mbc', 'sbs'],
        car_names = [
            "granduer",
            "avante",
            "palisade",
            "casper",
            "genesis",
            "ioniq"
        ],
        time_ranges = [
            {
                'start_datetime':'2016-08-28 21:00',
                'end_datetime'  :'2021-08-23 12:59'
            }
        ]
    )

    print(lambda_handler(event, None))