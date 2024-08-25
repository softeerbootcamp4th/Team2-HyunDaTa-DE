import boto3
import pandas as pd
from io import StringIO

'''
{
    "time_ranges": [
        {"start_datetime": "2024-08-17 15:00", "end_datetime": "2024-08-17 20:59"},
        {"start_datetime": "2024-08-17 09:00", "end_datetime": "2024-08-17 14:59"},
        {"start_datetime": "2024-08-17 03:00", "end_datetime": "2024-08-17 08:59"},
        {"start_datetime": "2024-08-16 21:00", "end_datetime": "2024-08-17 02:59"}
    ],
    "communities": [
        "dcinside"
    ],
    "car_names": [
        "avante", "palisade"
    ]
}

time_ranges가 존재한다면 time_ranges의 내용은 무결하다고 간주합니다.
무결하다: 각각의 start_datetime과 end_datetime 쌍으로 만들어진 범위들이 서로 겹치지 않고 연속한다.
'''

s3_client = boto3.client('s3')
bucket_name = "hyundata2-testbucket"

def make_object_key(community, car_name, start_datetime, end_datetime):
    object_key_base = "monitor/community/%s/%s/%s_%s_%s_%s.csv"

    date_str = start_datetime.strftime('%Y%m%d')
    start_dt_str = start_datetime.strftime('%Y%m%d_%H%M')
    end_dt_str = end_datetime.strftime('%Y%m%d_%H%M')
    object_key = object_key_base % (community, date_str, start_dt_str, end_dt_str, community, car_name)
    return object_key

def read_s3_file_as_dataframe(bucket_name, object_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        return df
    except s3_client.exceptions.NoSuchKey:
        print(f"[ERROR] The file does not exist: {bucket_name}:{object_key}")
    except s3_client.exceptions.ClientError as e:
        print(f"[ERROR] Client error when accessing the file: {e}")
    except Exception as e:
        print(f"[ERROR] Failed to read the file: {e}")
    return None

def load_df_to_s3(df, bucket_name, object_key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    try:
        s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
        msg = (
            f"The results were successfully merged and loaded at [{bucket_name}:{object_key}]"
        )
        return True, msg
    except Exception as e:
        return False, str(e)
    
def move_file_to_archive(bucket_name, object_keys):
    for object_key in object_keys:
        try:
            copy_source = {'Bucket': bucket_name, 'Key': object_key}
            path_parts = object_key.split('/')
            directory = '/'.join(path_parts[:-1])
            filename = path_parts[-1]
            archive_key = f"{directory}/archive/{filename}"

            s3_client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=archive_key)
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)

            print(f"[INFO] Moved {object_key} to {archive_key}")
        except s3_client.exceptions.NoSuchKey:
            print(f"[ERROR] The file does not exist when trying to move: {bucket_name}:{object_key}")
        except s3_client.exceptions.ClientError as e:
            print(f"[ERROR] Client error when moving the file: {e}")
        except Exception as e:
            print(f"[ERROR] Failed to move {object_key} to bin: {e}")

def lambda_handler(event, context):
    community = event.get('community')
    car_name = event.get('car_name')
    time_ranges = event.get('time_ranges')

    if not community: raise ValueError("Paremeter 'community' is not specified.")
    if not car_name: raise ValueError("Paremeter 'car_name' is not specified.")
    if not time_ranges: raise ValueError("Paremeter 'time_ranges' is not specified.")

    # 시간 범위의 가장 이른 시간과 가장 늦은 시간
    merged_start_datetime = min(pd.to_datetime(tr['start_datetime']) for tr in time_ranges)
    merged_end_datetime = max(pd.to_datetime(tr['end_datetime']) for tr in time_ranges)

    merged_df = pd.DataFrame()
    accessible_object_keys = []

    # 각 시간 범위에 대해 파일을 읽고 merged_df에 concatenate
    for time_range in time_ranges:
        start_datetime = pd.to_datetime(time_range['start_datetime'])
        end_datetime = pd.to_datetime(time_range['end_datetime'])
        object_key = make_object_key(community, car_name, start_datetime, end_datetime)
        df = read_s3_file_as_dataframe(bucket_name, object_key)
        if df is not None:
            merged_df = pd.concat([merged_df, df], ignore_index=True)
            accessible_object_keys.append(object_key)
        else:
            print(f"[ERROR] The file is missing or inaccessible. - {bucket_name}:{object_key}")

    # 병합된 데이터프레임을 날짜와 시간 기준으로 정렬
    if not merged_df.empty:
        merged_df = merged_df.sort_values(by=['Date', 'Time'])

    merged_object_key = make_object_key(
        community, car_name,
        merged_start_datetime, merged_end_datetime
    )

    # 병합된 데이터프레임을 S3 버킷에 업로드
    success, msg = load_df_to_s3(merged_df, bucket_name, merged_object_key)
    if success:
        print(f"[INFO] Merged file successfully loaded at {bucket_name}:{merged_object_key}")
        # 업로드 성공 시, 접근 가능한 오브젝트들을 아카이브로 이동
        move_file_to_archive(bucket_name, accessible_object_keys)
    else:
        print(f"[ERROR] Failed to load merged file: {msg}")

    return {
        "statusCode": 200,
        "message": "Files processed successfully. However, errors may have occurred during processing."
    }
