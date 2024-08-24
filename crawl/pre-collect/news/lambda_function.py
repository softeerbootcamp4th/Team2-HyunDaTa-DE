import sys
import io
from datetime import datetime
import json
import boto3

from crawler.kbs_crawler import KbsNewsCrawler
from crawler.mbc_crawler import MbcNewsCrawler
from crawler.sbs_crawler import SbsNewsCrawler


KOREAN_CAR_NAME = {
    'avante':'아반떼',
    'casper':'캐스퍼',
    'genesis':'제네시스',
    'granduer':'그랜저',
    'ioniq':'아이오닉',
    'palisade':'팰리세이드'
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
        news = event.get('news')
        query = event.get('car_name')
        start_datetime = datetime.strptime(event.get('start_datetime'), "%Y-%m-%d %H:%M")
        end_datetime = datetime.strptime(event.get('end_datetime'), "%Y-%m-%d %H:%M")

        print(news, query, start_datetime, end_datetime)

        if not news: return {'statusCode': 400, 'body': json.dumps('Error: news is not specified.')}
        if not query: return {'statusCode': 400, 'body': json.dumps('Error: Car name is not specified.')}
        if not start_datetime: return {'statusCode': 400, 'body': json.dumps('Error: Start datetime is not specified.')}
        if not end_datetime: return {'statusCode': 400, 'body': json.dumps('Error: End datetime is not specified.')}
        
        query_kor = get_query_in_korean(query)

        ### 크롤링 ###
        if news == 'kbs':
            crawler = KbsNewsCrawler(query_kor, start_datetime, end_datetime)
        elif news == 'mbc':
            crawler = MbcNewsCrawler(query_kor, start_datetime, end_datetime)
        elif news == 'sbs':            
            crawler = SbsNewsCrawler(query_kor, start_datetime, end_datetime)
        else:
            return {'statusCode': 400, 'body': json.dumps(f'Error: Unknown news media "{news}". Selecct one of [bobaedream, dcinside, naver_cafe].')}
        
        df = crawler.crawl_news()
        crawler.close()
        
        ### S3 업로드 ###
        object_name = f"monitor/news/{news}/{start_datetime.strftime('%Y%m%d')}/{start_datetime.strftime('%Y%m%d_%H%M')}_{end_datetime.strftime('%Y%m%d_%H%M')}_{news}_{query}.csv"
        upload_result, msg = upload_df_to_s3(df, "hyundata2-testbucket", object_name)
        if not upload_result:
            return {'statusCode': 500, 'body': json.dumps(f"Error: Failed to load at S3\n{msg}")}

        return {'statusCode': 200, 'body': json.dumps(msg)}

    except ValueError as ve:
        return {'statusCode': 400, 'body': json.dumps(str(ve))}
    except Exception as e:
        return {'statusCode': 500, 'body': json.dumps(f"Error: Unknown error occured. {str(e)}")}


if __name__== "__main__":
    news = sys.argv[1]
    car_name = sys.argv[2]
    start_datetime = sys.argv[3]
    end_datetime = sys.argv[4]
    
    event = dict(
        news=news,
        car_name=car_name,
        start_datetime=start_datetime,
        end_datetime=end_datetime
    )

    print(lambda_handler(event, None))
