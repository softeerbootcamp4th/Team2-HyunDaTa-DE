from datetime import datetime, timedelta

def lambda_handler(event, context):
    start_hours_ago = event.get('start_hours_ago', 0)
    duration_hours = event.get('duration_hours', 0)

    if start_hours_ago <= 0:
        raise Exception("[ERROR] start_hours_ago is not specified or inappropriate value.")
    if duration_hours <= 0:
        raise Exception("[ERROR] duration_hours is not specified or inappropriate value.")

    now_utc0 = datetime.now()
    now_utc9 = now_utc0 + timedelta(hours=9)
    now_utc9_no_minute = now_utc9.replace(minute=0, second=0, microsecond=0)
    start_datetime = now_utc9_no_minute - timedelta(hours=start_hours_ago)
    end_datetime = start_datetime + timedelta(hours=duration_hours, minutes=-1)

    return {
        "start_datetime": start_datetime.strftime('%Y-%m-%d %H:%M'),
        "end_datetime": end_datetime.strftime('%Y-%m-%d %H:%M')
    }