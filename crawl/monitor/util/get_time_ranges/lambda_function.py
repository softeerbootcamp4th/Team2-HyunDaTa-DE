from datetime import datetime, timedelta

def lambda_handler(event, context):
    hours = event.get('hours', 0)
    num_splits = event.get('num_splits', 0)

    if hours <= 0:
        raise Exception("[ERROR] 'hours' is not specified or not a positive value.")
    if num_splits <= 0:
        raise Exception("[ERROR] 'num_splits' is not specified or not a positive value.")

    time_splits = [hours // num_splits + (i < hours % num_splits) for i in range(num_splits)]

    time_ranges = []
    start_hours_ago = 0
    for split in time_splits:
        start_hours_ago += split

        now_utc0 = datetime.now()
        now_utc9 = now_utc0 + timedelta(hours=9)
        now_utc9_no_minute = now_utc9.replace(minute=0, second=0, microsecond=0)
        start_datetime = now_utc9_no_minute - timedelta(hours=start_hours_ago)
        end_datetime = start_datetime + timedelta(hours=split, minutes=-1)

        time_ranges.append({
            "start_datetime": start_datetime.strftime('%Y-%m-%d %H:%M'),
            "end_datetime": end_datetime.strftime('%Y-%m-%d %H:%M')
        })
        print(f"[INFO] range: {start_datetime} ~ {end_datetime}")

    return {
        "time_ranges": time_ranges
    }



if __name__ == "__main__":
    # 예시 이벤트 입력에 대해 테스트
    event = {
        "hours":24,
        "num_splits":4
    }

    print(lambda_handler(event=event, context=None))