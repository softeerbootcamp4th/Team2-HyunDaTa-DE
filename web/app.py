from flask import Flask, jsonify, request, render_template
import pymysql
import pandas as pd
import logging
from datetime import datetime, timedelta
import json
import boto3
from apscheduler.schedulers.background import BackgroundScheduler
import os
import yaml

app = Flask(__name__, static_folder='', static_url_path='', template_folder='')

scheduler = BackgroundScheduler()

db_config = {
    'host': 'host',
    'user': 'user',
    'password': 'password',
    'database': 'database'
}
sns_client = boto3.client('sns', region_name='region_name')
sns_topic_arn = 'sns_topic_arn'

logging.basicConfig(level=logging.DEBUG)

with open("config.yaml", "r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

valid_car_names = config.get('car_name', [])


def generate_subject_and_message(recent_issues):
    now = datetime.now().strftime('%Y-%m-%d %I%p')
    subject = f"[HyunDaTa Alert] {now} Hot Issues"
    message_lines = []
    for issue in recent_issues:
        car_name = issue['car_name']
        single_issue = issue['single_issue']
        url = f"http://127.0.0.1:5000/index.html?exception_mode=hot_issue&car_name={car_name}&issue={single_issue}"
        message_lines.append(f"{car_name} - {single_issue}\n{url}\n\n\n")

    message_content = "\n".join(message_lines)
    message = f"""
    안녕하십니까. {now} Community 모니터링 결과 보고드립니다.

    현재 총 {len(recent_issues)}건의 Hot Issue가 탐지되었습니다.\n\n\n{message_content}

    감사합니다.
    HyunDaTa 드림.
    """
    message_json = {
        "default": "Recent Triggered Issues",
        "email": message.strip(),
        "lambda": message.strip()
    }
    return subject, json.dumps(message_json)

def generate_news_alert_message(recent_news):
    now = datetime.now().strftime('%Y-%m-%d %I%p')
    subject = f"[HyunDaTa Alert] {now} 뉴스 보도 알림"
    message_lines = []

    for news in recent_news:
        car_name = news['car_name']
        single_issue = news['issue']
        title = news['title']
        summary = news['summary']
        url = news['url']

        message_lines.append(
            f"차량명: {car_name}\n이슈: {single_issue}\n제목: {title}\n요약: {summary}\nurl: {url}\n\n"
        )

    message_content = "\n".join(message_lines)
    message = f"""
    안녕하십니까. {now} 뉴스 모니터링 결과 보고드립니다.

    현재 총 {len(recent_news)}건의 뉴스가 보도되었습니다.\n\n{message_content}

    감사합니다.
    HyunDaTa 드림.
    """
    
    message_json = {
        "default": "Recent News Alerts",
        "email": message.strip(),
        "lambda": message.strip()
    }
    
    return subject, json.dumps(message_json)

def send_recent_news_to_sns(news_data):
    last_5_days = datetime.now() - timedelta(days=5)
    recent_issues = []
    for item in news_data:
        for config_car_name in valid_car_names:
            if item['upload_date'] >= last_5_days.date() and config_car_name in item['car_name']:
                recent_issues.append({
                    'car_name': item['car_name'], 
                    'issue': item['issue'], 
                    'upload_date': item['upload_date'], 
                    'summary': item['summary'], 
                    'title': item['title'], 
                    'url': item['url']
                })
    if recent_issues:
        subject, message_json = generate_news_alert_message(recent_issues)
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message_json,
            Subject=subject,
            MessageStructure='json'
        )
        logging.debug(f"SNS Publish Response: {response}")
    else:
        logging.debug("No recent issues to send.")

def send_recent_triggers_to_sns(trigger_data):
    last_5_days = datetime.now() - timedelta(days=5)
    recent_issues = []
    recent_issues = [
        {'car_name': item['car_name'], 'single_issue': item['issue'],
            'upload_date': item['upload_date']}
        for item in trigger_data
        if item['upload_date'] >= last_5_days.date()
    ]
    if recent_issues:
        subject, message_json = generate_subject_and_message(recent_issues)
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message_json,
            Subject=subject,
            MessageStructure='json'
        )
        logging.debug(f"SNS Publish Response: {response}")
    else:
        logging.debug("No recent issues to send.")


def get_data_from_rds():
    global trigger_data, recommend_data, today_data, news_data
    connection = None
    try:
        connection = pymysql.connect(**db_config)
        trigger_query = '''
            SELECT upload_date, car_name, issue, url
            FROM issue_trigger
            WHERE is_trigger = True
            ORDER BY upload_date DESC
        '''
        trigger_data = pd.read_sql(
            trigger_query, connection).to_dict(orient='records')

        recommend_query = '''
            SELECT monitor_car_name, monitor_issue, monitor_graph_score_list, recommend_json
            FROM recommend
        '''
        recommend_data = pd.read_sql(
            recommend_query, connection).to_dict(orient='records')

        today_query = '''
            SELECT 
                p.upload_date,
                p.car_name, 
                JSON_UNQUOTE(JSON_EXTRACT(p.issue, CONCAT('$[', numbers.n, ']'))) AS single_issue,
                COUNT(*) AS num_posts, 
                SUM(p.num_likes) AS total_likes, 
                SUM(p.num_views) AS total_views,
                COUNT(*) + 
                SUM(
                    CHAR_LENGTH(CONCAT(p.title, p.body)) - 
                    CHAR_LENGTH(REPLACE(CONCAT(p.title, p.body), JSON_UNQUOTE(JSON_EXTRACT(p.issue, CONCAT('$[', numbers.n, ']'))), ''))
                ) / CHAR_LENGTH(JSON_UNQUOTE(JSON_EXTRACT(p.issue, CONCAT('$[', numbers.n, ']')))) AS issue_mentions,
                it.url AS issue_url
            FROM 
                post p
            JOIN 
                (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) numbers
            ON 
                JSON_LENGTH(p.issue) > numbers.n
            LEFT JOIN 
                issue_trigger it ON p.car_name = it.car_name AND JSON_UNQUOTE(JSON_EXTRACT(p.issue, CONCAT('$[', numbers.n, ']'))) = it.issue
            WHERE 
                DATE(p.upload_date) >= CURDATE() - INTERVAL 5 DAY
                AND JSON_UNQUOTE(JSON_EXTRACT(p.issue, CONCAT('$[', numbers.n, ']'))) IS NOT NULL
            GROUP BY 
                p.car_name, single_issue;
        '''

        today_data = pd.read_sql(
            today_query, connection).to_dict(orient='records')

        news_query = '''
            SELECT *
            FROM news
        '''
        news_data = pd.read_sql(
            news_query, connection).to_dict(orient='records')
        send_recent_triggers_to_sns(trigger_data)
        send_recent_news_to_sns(news_data)
        logging.debug("Data successfully loaded from RDS.")
        return trigger_data, recommend_data, today_data, news_data

    except Exception as e:
        logging.error(f"Database connection or query failed: {e}")
        return None, None, None, None

    finally:
        if connection:
            connection.close()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/exception_mode=hot_issue')
def api_hot_issue():
    last_1_days = datetime.now() - timedelta(days=10)
    recent_issues = [
        f"{item['car_name']} - {item['issue']}\n({item['upload_date']})" for item in trigger_data
        if item['upload_date'] >= last_1_days.date()
    ]
    return jsonify(recent_issues)


@app.route('/api/data')
def api_data():
    car_name = request.args.get('car_name')
    issue = request.args.get('issue')

    if not car_name or not issue:
        logging.debug(
            "Invalid parameters received: car_name=%s, issue=%s", car_name, issue)
        return jsonify({"error": "Invalid parameters"}), 400

    specific_today_data = next(
        (item for item in today_data if item['car_name'] == car_name and item['single_issue'] == issue), None)

    if recommend_data is None:
        logging.error(
            "Recommend data is None. There was a problem loading data from the database.")
        return jsonify({"error": "Internal server error, no data available"}), 500

    data = next((item for item in recommend_data if item['monitor_car_name']
                == car_name and item['monitor_issue'].upper() == issue.upper()), None)

    if data is None:
        logging.debug(
            "No valid data found for the given car_name=%s and issue=%s", car_name, issue)
        return jsonify({"error": "No data found"}), 404

    monitor_data = json.loads(data['monitor_graph_score_list']) if isinstance(
        data['monitor_graph_score_list'], str) else data['monitor_graph_score_list']
    recommend_data_list = json.loads(data['recommend_json']) if isinstance(
        data['recommend_json'], str) else data['recommend_json']

    logging.debug("Monitor Data Load Success")

    all_dates = set()
    for rec in recommend_data_list:
        if 'recommend_graph_score_list' in rec:
            for entry in rec['recommend_graph_score_list']:
                all_dates.add(entry['upload_date'])

    for entry in monitor_data:
        all_dates.add(entry['upload_date'])

    recommend_dates = sorted(
        [datetime.strptime(date, '%Y-%m-%d').date() for date in all_dates])

    logging.debug("Recommend Data Load Success")

    datasets = []
    vertical_lines = []

    monitor_data_dict = {
        entry['upload_date']: entry['norm_graph_score'] for entry in monitor_data}
    aligned_monitor_scores = [monitor_data_dict.get(
        date.strftime('%Y-%m-%d'), None) for date in recommend_dates]

    datasets.append({
        "label": f"Monitoring {car_name} - {issue}",
        "data": aligned_monitor_scores,
        "borderColor": "rgba(255, 0, 0, 1)",
        "fill": False
    })

    colors = [
        "rgba(0, 128, 0, 0.4)",
        "rgba(0, 0, 128, 0.4)",
        "rgba(255, 255, 0, 0.5)",
    ]

    for i, rec in enumerate(recommend_data_list):
        if 'recommend_graph_score_list' in rec:
            rec_data_dict = {entry['upload_date']: entry['norm_graph_score']
                             for entry in rec['recommend_graph_score_list']}
            aligned_rec_scores = [rec_data_dict.get(date.strftime(
                '%Y-%m-%d'), None) for date in recommend_dates]

            datasets.append({
                "label": f"{rec['recommend_car_name']} - {rec['recommend_issue']} ({rec['recommend_start_date']})",
                "data": aligned_rec_scores,
                "borderColor": colors[i % len(colors)],
                "fill": False,
            })

            start_date = datetime.strptime(
                rec['recommend_start_date'], '%Y-%m-%d').date()
            end_date = start_date + timedelta(days=90)

            for news_item in news_data:
                news_date = news_item['upload_date']

                if isinstance(news_date, str):
                    news_date = datetime.strptime(news_date, '%Y-%m-%d').date()

                if start_date <= news_date <= end_date and rec['recommend_car_name'] in news_item['car_name'] and rec['recommend_issue'] == news_item['issue']:
                    date_diff = news_date - start_date

                    _tmp_date = (datetime.now().date() +
                                 date_diff).strftime('%Y-%m-%d')

                    vertical_lines.append({
                        "date": _tmp_date,
                        "color": colors[i % len(colors)],
                        "label": f'news: {news_item["car_name"]} - {news_item["issue"]}'
                    })
    matching_trigger = next(
        (item for item in trigger_data if item['car_name'] == car_name and item['issue'] == issue), None)
    trigger_url = None
    if matching_trigger and matching_trigger['url']:
        url_list = json.loads(matching_trigger['url']) if isinstance(
            matching_trigger['url'], str) else matching_trigger['url']
        trigger_url = url_list[0] if isinstance(url_list, list) and url_list else None

    chart_data = {
        "labels": [date.strftime('%Y-%m-%d') for date in recommend_dates],
        "datasets": datasets,
        "vertical_lines": vertical_lines,
        "today_data": specific_today_data,
        "url": trigger_url
    }

    logging.debug("Chart Data Load Success")

    return jsonify(chart_data)


@app.route('/api/today_data')
def api_today_data():
    if today_data is None:
        return jsonify({"error": "No data found"}), 500

    enriched_today_data = []
    for item in today_data:
        enriched_today_data.append({
            "upload_date": item['upload_date'],
            "car_name": item['car_name'],
            "single_issue": item['single_issue'],
            "num_posts": item['num_posts'],
            "total_likes": item['total_likes'],
            "total_views": item['total_views'],
            "issue_mentions": item['issue_mentions'],
            "url": item.get('issue_url')
        })

    return jsonify(enriched_today_data)


@app.route('/hot_issue_redirect')
def hot_issue_redirect():
    car_name = request.args.get('car_name')
    issue = request.args.get('issue')
    if car_name and issue:
        return redirect(url_for('index', car_name=car_name, issue=issue))
    return redirect(url_for('index'))

@app.route('/api/config')
def get_config():
    with open('config.yaml', 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return jsonify(config)

trigger_data = []
recommend_data = []
today_data = []
news_data = []

if not scheduler.get_jobs():
    scheduler.add_job(get_data_from_rds, 'cron', minute=0)
    scheduler.start()

if __name__ == '__main__':
    if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        get_data_from_rds()
    app.run(debug=True)
