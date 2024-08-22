import os
import re
import json
import boto3
import pandas as pd
import pymysql
from dotenv import load_dotenv

# rds.env 파일의 경로를 지정하여 환경 변수 불러오기
load_dotenv('.env')

# 환경 변수에서 RDS 정보 불러오기
rds_host = os.getenv('RDS_HOST')
rds_user = os.getenv('RDS_USER')
rds_password = os.getenv('RDS_PASSWORD')
rds_database = os.getenv('RDS_DATABASE')
buekct_name = os.getenv('BUCKET_NAME')
s3 = boto3.client('s3')
with open('defect_keywords.json', 'r', encoding='utf-8') as file:
    defect_keywords = json.load(file)


# S3에서 CSV 파일들을 읽어 DataFrame으로 반환하는 함수
def read_csvs_in_s3(communities: list, car_names: list, start_datetime, end_datetime) -> pd.DataFrame:
    all_data = []
    
    for community in communities:
        for car_name in car_names:
            s3_key = f"monitor/community/{community}/{start_datetime.strftime("%Y%m%d")}/{start_datetime.strftime("%Y%m%d_%H%M")}_{end_datetime.strftime("%Y%m%d_%H%M")}_{community}_{car_name}.csv"
            
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

# RDS의 PostContents 테이블을 업데이트하는 함수
def update_rds_table(df):
    connection = connect_to_rds()
    
    insert_update_query = """
        INSERT INTO post (upload_date, upload_time, title, body, comments, num_views, num_likes, community, car_name, issue, url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS new
        ON DUPLICATE KEY UPDATE
            comments = new.comments,
            num_views = new.num_views,
            num_likes = new.num_likes;
    """
    
    data_to_insert = [
        (
            row['Date'], 
            row['Time'], 
            row['Title'], 
            row['Body'], 
            row['Comment'], 
            row['View'],
            row['Like'],
            row['Community'],
            row['CarName'],
            row['Issue'],
            row['Url']
        )
        for _, row in df.iterrows()
    ]
    
    with connection.cursor() as cursor:
        cursor.executemany(insert_update_query, data_to_insert)
    
    connection.commit()
    connection.close()

# 숫자 데이터의 콤마 제거 및 단위 환산하는 함수
def clean_numeric_value(value):
    if isinstance(value, str):
        value = value.replace(',', '')
        if '만' in value:
            value = re.sub(r'(\d+\.?\d*)만', lambda x: str(int(float(x.group(1)) * 10000)), value)
    return int(float(value))

# URL에서 검색 쿼리 제거하는 함수
def remove_search_query_in_url(row):
    url = row['Url']
    community = row['Community']
    if community == 'dcinside':
        url = re.sub(r'&search_pos=.*', '', url)
    elif community == 'bobaedream':
        url = re.sub(r'&bm=.*', '', url)
    return url

def defect_detection(text):
    defects = []
    for item in defect_keywords:
        defect_type, keywords = item['defect_type'], item['keywords']
        for keyword in keywords:
            if keyword in text:
                defects.append(defect_type)
                break
    return json.dumps(defects)


def clean_df(df):
    df = df.fillna('')
    df['Date'] = df['Date'].apply(lambda x: x.split()[0])
    df['View'] = df['View'].apply(clean_numeric_value)
    df['Like'] = df['Like'].apply(clean_numeric_value)
    df['Issue'] = (df['Title'] + df['Body']).apply(defect_detection)
    df['Url'] = df[['Community','Url']].apply(remove_search_query_in_url, axis=1)
    df['CarName'] = df['CarName'].apply(lambda x: '팰리세이드' if x == '리세이드' else x)

    df = df.sort_values(by=['Date', 'Time', 'View'], ascending=[True, True, False]).reset_index(drop=True) # 크롤링을 놓친 데이터가 있을 수 있기 때문에 반드시 시간순으로 db에 저장됨을 보장하지는 못함
    df = df.drop_duplicates('Url', keep='first') # 'Url'이 같은 행이 있다면 'View'가 큰 행을 남기고 나머지 드랍
    
    return df

def lambda_handler(event, context):
    communities = event.get('communities')
    car_names = event.get('car_names')
    time_ranges = event.get('time_ranges')

    start_datetime = pd.to_datetime(min(tr['start_datetime'] for tr in time_ranges))
    end_datetime = pd.to_datetime(max(tr['end_datetime'] for tr in time_ranges))
    
    df = read_csvs_in_s3(communities, car_names, start_datetime, end_datetime)
    
    if df.empty:
        print("No crawled data")
        return {
            'statusCode': 200,
            'body': 'RDS table was not updated, but it ran successfully'
        }

    df = clean_df(df)
    print("Number of crawled posts:", df.shape[0])
    update_rds_table(df)
    return {
        'statusCode': 200,
        'body': 'RDS table updated successfully'
    }



if __name__ == "__main__":
    event = dict(
        communities = ['bobaedream'],
        car_names = [
            # "granduer",
            "avante",
            # "palisade",
            # "casper",
            # "genesis",
            # "ioniq"
        ],
        time_ranges = [
            {
                'start_datetime':'2024-08-17 15:00', 
                'end_datetime'  :'2024-08-19 14:59'
            }
        ]
    )
    lambda_handler(event, None)