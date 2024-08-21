import io
from datetime import datetime
import json
import boto3

from crawler.bobaedream_crawler import BobaedreamCrawler
from crawler.dcinside_crawler import DcInsideCrawler
from crawler.naver_cafe_crawler import NaverCafeCralwer


KOREAN_CAR_NAME = {
    'avante':'아반떼',
    'casper':'캐스퍼',
    'genesis':'제네시스',
    'granduer':'그랜저',
    'ioniq':'아이오닉',
    'palisade':'리세이드', # 팰리세이드를 펠리세이드로 적는 사람이 많음
}

def upload_df_to_s3(df, bucket_name, object_name):
    s3 = boto3.client('s3')

    # 데이터프레임을 CSV로 변환하여 메모리에서 처리
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    try:
        s3.upload_fileobj(Fileobj=csv_buffer, Bucket=bucket_name, Key=object_name)
        msg = (
            f"Total {len(df)} post are crawled.\n"
            + f"The data is successfully loaded at [{bucket_name}:{object_name}].\n"
        )
        return True, msg
    except Exception as e:
        return False, str(e)

def get_query_in_korean(query):
    try:
        return KOREAN_CAR_NAME[query]
    except KeyError:
        raise ValueError(f'Unknown car name "{query}".')

def lambda_handler(event, context):
    try:
        ### 파라미터 확인 ###
        community = event.get('community')
        query = event.get('car_name')
        start_datetime = datetime.strptime(event.get('start_datetime'), "%Y-%m-%d %H:%M")
        end_datetime = datetime.strptime(event.get('end_datetime'), "%Y-%m-%d %H:%M")

        print(community, query, start_datetime, end_datetime)

        if not community: return {'statusCode': 400, 'body': json.dumps('Error: community is not specified.')}
        if not query: return {'statusCode': 400, 'body': json.dumps('Error: Car name is not specified.')}
        if not start_datetime: return {'statusCode': 400, 'body': json.dumps('Error: Start datetime is not specified.')}
        if not end_datetime: return {'statusCode': 400, 'body': json.dumps('Error: End datetime is not specified.')}
        
        query_kor = get_query_in_korean(query)

        ### 크롤링 ###
        if community == 'bobaedream':
            crawler = BobaedreamCrawler()
            df = crawler.bobaedream_crawl(query_kor, start_datetime, end_datetime)
        elif community == 'dcinside':
            crawler = DcInsideCrawler(query_kor, start_datetime, end_datetime)
            df = crawler.start_crawling(num_processes=1)
        elif community == 'naver_cafe':            
            crawler = NaverCafeCralwer()
            crawler.set_current_crawl_option(query)
            df = crawler.start_crawling(end_datetime)
        else:
            return {'statusCode': 400, 'body': json.dumps(f'Error: Unknown community "{community}". Selecct one of [bobaedream, dcinside, naver_cafe].')}
        
        ### S3 업로드 ###
        object_key = f"monitor/community/{community}/{start_datetime.strftime('%Y%m%d')}/{start_datetime.strftime('%Y%m%d_%H%M')}_{end_datetime.strftime('%Y%m%d_%H%M')}_{community}_{query}.csv"
        upload_result, msg = upload_df_to_s3(df, "hyundata2-testbucket", object_key)
        if not upload_result:
            return {'statusCode': 500, 'body': json.dumps(f"Error: Failed to load at S3\n{msg}")}

        return {'statusCode': 200, 'body': json.dumps(msg)}

    except ValueError as ve:
        return {'statusCode': 400, 'body': json.dumps(str(ve))}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error: Unknown error occured. {str(e)}")}