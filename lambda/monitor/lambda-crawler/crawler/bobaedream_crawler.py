import sys
import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from datetime import datetime
from crawler.driver import get_driver

URL = dict(
  free="https://www.bobaedream.co.kr/list?code=freeb&s_cate=&maker_no=&model_no=&or_gu=10&or_se=desc&s_selday=&pagescale=70&info3=&noticeShow=&s_select=Body&level_no=&vdate=&type=list",
  domestic="https://www.bobaedream.co.kr/list?code=national&s_cate=&maker_no=&model_no=&or_gu=10&or_se=desc&s_selday=&pagescale=70&info3=&noticeShow=&s_select=Body&s_key=&level_no=&vdate=&type=list"
)

class BobaedreamCrawler:
    def __init__(self):
        self.driver = get_driver()

    @staticmethod
    def parse_datetime(datetime_str):
        try:
            return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M")
        except ValueError:
            print(f"Invalid datetime format: {datetime_str}. Use YYYY-MM-DD HH:MM")
            sys.exit(1)

    def fetch_page_source(self, url, retries=3):
        for attempt in range(retries):
            try:
                self.driver.get(url)
                return self.driver.page_source
            except TimeoutException:
                print(f"TimeoutException for URL: {url}, attempt {attempt + 1}/{retries}")
                time.sleep(0.5)
        return None

    def extract_article_data(self, post_url, title, start_date, end_date, car_name):
        page_source = self.fetch_page_source(post_url)
        if not page_source:
            return None

        soup = BeautifulSoup(page_source, 'html.parser')
        body_content = soup.find('div', {'class': 'bodyCont'})
        body_text = body_content.get_text(strip=True) if body_content else ''

        date_time_element = soup.find('span', {'class': 'countGroup'})
        if date_time_element:
            em_elements = date_time_element.find_all('em', {'class': 'txtType'})
            date_time_text = date_time_element.get_text(strip=True)
            date_time_match = re.search(r'(\d{4}\.\d{2}\.\d{2})\s*\(\w+\)\s*(\d{2}:\d{2})', date_time_text)

            view = em_elements[0].get_text(strip=True) if len(em_elements) > 0 else ''
            like = em_elements[1].get_text(strip=True) if len(em_elements) > 1 else ''
            news_date_str = date_time_match.group(1) if date_time_match else ''
            news_time_str = date_time_match.group(2) if date_time_match else ''

            news_datetime_str = f"{news_date_str} {news_time_str}"

            try:
                news_datetime = datetime.strptime(news_datetime_str, "%Y.%m.%d %H:%M")
            except ValueError:
                print(f"Date format error: {news_datetime_str}")
                news_datetime = None
            if news_datetime is None or news_datetime > end_date:
                return 'PASS'
            if news_datetime < start_date:
                return 'STOP'

        comments = soup.find_all('dd', {'id': lambda x: x and x.startswith('small_cmt_')})
        comment_texts = [comment.get_text(strip=True) for comment in comments]
        comments_string = ' '.join(comment_texts)
        formatted_date = news_datetime.strftime('%Y-%m-%d') if news_datetime else ''

        return {
            'Date': formatted_date, 
            'Time': news_time_str, 
            'Title': title, 
            'Body': body_text, 
            'Comment': comments_string, 
            'View': view, 
            'Like': like, 
            'Community': 'bobaedream', 
            'CarName': car_name, 
            'Url': post_url
        }

    def extract_articles_from_page(self, page_source):
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

    def process_page(self, search_url, start_datetime, end_datetime, car_name):
        page_source = self.fetch_page_source(search_url)
        if not page_source:
            return []

        article_data_list = self.extract_articles_from_page(page_source)
        return [(post_url, title, start_datetime, end_datetime, car_name) for post_url, title in article_data_list]

    def bobaedream_crawl(self, car_name, start_datetime, end_datetime):
        result_df = pd.DataFrame(columns=['Date', 'Time', 'Title', 'Body', 'Comment', 'View', 'Like', 'Community', 'CarName', 'Url'])

        try:
            for board_name, base_url in URL.items():
                current_page = 1
                stop_crawling = False
                while not stop_crawling:
                    search_url = f"{base_url}&s_key={car_name}&page={current_page}"
                    article_data_list = self.process_page(search_url, start_datetime, end_datetime, car_name)
                    if not article_data_list:
                        break

                    for post_url, title, start_datetime, end_datetime, car_name in article_data_list:
                        data = self.extract_article_data(post_url, title, start_datetime, end_datetime, car_name)
                        if data == 'STOP':
                            stop_crawling = True
                            break
                        elif data == 'PASS':
                            continue
                        elif data:
                            result_df = pd.concat([result_df, pd.DataFrame([data])], ignore_index=True)

                    current_page += 1

            if not result_df.empty:
                return result_df
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            self.driver.quit()

        print("Filtered video data has been successfully saved.")
        return result_df
