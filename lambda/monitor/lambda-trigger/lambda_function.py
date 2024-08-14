from datetime import datetime, timedelta
import json
import boto3

TIME_RANGE = 6 # 검색할 시간 범위
CAR_NAME_LIST = ['granduer', 'avante', 'palisade', 'casper', 'genesis', 'ioniq']
COMMUNITY_LIST = ['dcinside', 'bobaedream'] #, 'naver_cafe']

def get_crawling_time_range(time_range=1):
    now_utc0 = datetime.now()
    now_utc9 = now_utc0 + timedelta(hours=9)
    now_utc9_no_minute = now_utc9.replace(minute=0, second=0, microsecond=0)
    start_datetime = now_utc9_no_minute - timedelta(hours=time_range)
    end_datetime = now_utc9_no_minute - timedelta(minutes=1)
    return start_datetime, end_datetime

def lambda_handler(event, context):
    lambda_client = boto3.client('lambda')
    start_datetime, end_datetime = get_crawling_time_range(time_range=TIME_RANGE)
    start_datetime_str = start_datetime.strftime('%Y-%m-%d %H:%M')
    end_datetime_str = end_datetime.strftime('%Y-%m-%d %H:%M')
    
    results = []

    for car in CAR_NAME_LIST:
        for community in COMMUNITY_LIST:
            payload = {
                'car_name': car,
                'community': community,
                'start_datetime': start_datetime_str,
                'end_datetime': end_datetime_str
            }

            try:
                # 비동기 호출
                lambda_client.invoke(
                    FunctionName='monitor-community',
                    InvocationType='Event',  # 비동기 호출
                    Payload=json.dumps(payload)
                )

                # 비동기 호출의 경우 성공적으로 큐에 전달되었다는 정보만 기록할 수 있음
                results.append({
                    'car': car,
                    'community': community,
                    'status': 'Invoked'
                })

                print(f"Successfully invoked {car} in {community}.")
                
            except Exception as e:
                print(f"Error invoking Lambda for {car} in {community}: {str(e)}")
                results.append({
                    'car': car,
                    'community': community,
                    'error': str(e)
                })
    
    return {
        'statusCode': 200,
        'body': json.dumps(results)
    }
