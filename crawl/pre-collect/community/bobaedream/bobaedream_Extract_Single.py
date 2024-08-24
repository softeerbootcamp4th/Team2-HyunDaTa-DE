import yaml
import os
import sys
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import time
import pandas as pd
import re

# Config 파일 읽기
with open('bobaedream_config.yaml', 'r', encoding='utf-8') as file:
    config = yaml.safe_load(file)

# 크롬 드라이버 경로 설정
chrome_driver_path = "/Users/admin/Desktop/Data_Engineering/chromedriver"

# 크롬 옵션 설정
chrome_options = Options()
chrome_options.add_argument("--headless")  # 헤드리스 모드
chrome_options.add_argument("--disable-gpu")  # GPU 비활성화
chrome_options.add_argument("--window-size=1920x1080")  # 화면 크기 설정
chrome_options.add_argument("--no-sandbox")  # 샌드박스 비활성화
chrome_options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화

def init_driver():
    service = Service(chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(10)  # 타임아웃 시간 설정 (10초)
    return driver

def extract_article_data(post_url, title, start_date, end_date):
    driver = init_driver()
    try:
        driver.get(post_url)
        print(f"Accessing: {post_url} - {title}")  # 접속 확인 출력
        time.sleep(0.2)
        post_page_source = driver.page_source
        post_soup = BeautifulSoup(post_page_source, 'html.parser')
        body_content = post_soup.find('div', {'class': 'bodyCont'})
        body_text = body_content.get_text(strip=True) if body_content else ''

        date_time_element = post_soup.find('span', {'class': 'countGroup'})
        if date_time_element:
            em_elements = date_time_element.find_all('em', {'class': 'txtType'})
            date_time_text = date_time_element.get_text(strip=True)
            date_time_match = re.search(r'(\d{4}\.\d{2}\.\d{2})\s*\(\w+\)\s*(\d{2}:\d{2})', date_time_text)

            view = em_elements[0].get_text(strip=True) if len(em_elements) > 0 else ''
            like = em_elements[1].get_text(strip=True) if len(em_elements) > 1 else ''
            date = date_time_match.group(1) if date_time_match else ''
            time_of_day = date_time_match.group(2) if date_time_match else ''

            if date and date < start_date:
                return 'STOP'  # 시작 날짜 이전의 게시물 무시
            if not (start_date <= date <= end_date):
                return None  # 지정된 날짜 범위 밖의 게시물 무시

        comments = post_soup.find_all('dd', {'id': lambda x: x and x.startswith('small_cmt_')})
        comment_texts = [comment.get_text(strip=True) for comment in comments]
        comments_string = ' '.join(comment_texts)  # 스페이스 하나로 조인
        return {'Date': date, 'Time': time_of_day, 'Title': title, 'Body': body_text, 'Comment': comments_string, 'View': view, 'Like': like, 'Community': 'bobaedream'}

    except TimeoutException:
        print(f"TimeoutException for URL: {post_url}")
        return None
    finally:
        driver.quit()

def extract_articles_from_page(page_source):
    soup = BeautifulSoup(page_source, 'html.parser')
    articles = soup.find_all('tr', {'itemscope': '', 'itemtype': 'http://schema.org/Article'})

    if not articles:
        return []

    article_data = []
    for article in articles:
        title_element = article.find('a', {'class': 'bsubject'})
        if title_element:
            title = title_element.get_text(strip=True)
            post_url = "https://www.bobaedream.co.kr" + title_element['href']
            article_data.append((post_url, title))
    return article_data

def process_page(search_url, page, start_date, end_date):
    driver = init_driver()
    driver.get(search_url)
    time.sleep(0.2)
    page_source = driver.page_source
    driver.quit()
    article_data_list = extract_articles_from_page(page_source)
    return [(post_url, title, start_date, end_date) for post_url, title in article_data_list]

def save_to_csv(dataframe, result_dir, keyword):
    result_file = os.path.join(result_dir, f'bobaedream_{keyword}_Extract_Result.csv')
    if os.path.exists(result_file):
        dataframe.to_csv(result_file, mode='a', header=False, index=False, encoding='utf-8-sig')
    else:
        dataframe.to_csv(result_file, index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    # 터미널 입력 처리
    if len(sys.argv) != 3:
        print("Usage: python bobaedream.py <start_date> <end_date>")
        sys.exit(1)

    start_date = sys.argv[1]
    end_date = sys.argv[2]

    try:
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print("Date format should be YYYY-MM-DD")
        sys.exit(1)

    start_date = start_date.replace("-", ".")
    end_date = end_date.replace("-", ".")

    # 결과 저장 디렉토리 생성
    result_dir = 'bobaedream_Extract_Result'
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    try:
        for board_name, base_url in config['Community']['bobaedream'].items():
            for keyword in config['Keyword']['Car']:
                current_page = 1
                df = pd.DataFrame()  # 각 키워드마다 df 초기화
                stop_crawling = False
                while not stop_crawling:
                    search_url = f"{base_url}&s_key={keyword}&page={current_page}"
                    article_data_list = process_page(search_url, current_page, start_date, end_date)

                    if not article_data_list:
                        break  # 더 이상 게시물이 없으면 크롤링 종료

                    for post_url, title, start_date, end_date in article_data_list:
                        data = extract_article_data(post_url, title, start_date, end_date)
                        if data == 'STOP':
                            stop_crawling = True
                            break
                        elif data:
                            df = pd.concat([df, pd.DataFrame([data])], ignore_index=True)

                    # 일정 주기마다 저장
                    if len(df) >= 100:
                        save_to_csv(df, result_dir, keyword)
                        df = pd.DataFrame()  # 저장 후 데이터프레임 초기화

                    current_page += 1

                # 마지막으로 남은 데이터 저장
                if not df.empty:
                    save_to_csv(df, result_dir, keyword)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        print("Filtered video data has been successfully saved.")
