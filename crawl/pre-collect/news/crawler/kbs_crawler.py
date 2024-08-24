import sys
import time
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService

from crawler.driver import get_driver

class KbsNewsCrawler:
    def __init__(self, car_name, start_datetime, end_datetime):
        self.car_name = car_name
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.driver = get_driver()

    # Get Each News URL
    def get_news_links(self, query, page_num, max_retries=5):
        url = f"https://news.kbs.co.kr/news/pc/search/search.do?query={query}&sortType=date&filter=1&page={page_num}&rpage=1"
        print(f"Fetching news links from URL: {url}")  # Debugging print
        
        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                time.sleep(0.5)

                # Scroll down to load all news items
                last_height = self.driver.execute_script("return document.body.scrollHeight")
                while True:
                    self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(0.5)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        break
                    last_height = new_height
                
                # Extract news items
                news_items = self.driver.find_elements(By.CSS_SELECTOR, 'div.duplicate-articles-wrapper a.box-content')
                news_links = [item.get_attribute('href') for item in news_items]
                
                if news_links:
                    print(f"Found {len(news_links)} news links on page {page_num}")  # Debugging print
                else:
                    print(f"No news links found on page {page_num}")  # Debugging print
                
                return news_links
            except Exception as e:
                print(f"Error on attempt {attempt + 1} for page {page_num}: {e}")
                time.sleep(1)
        
        print(f"Failed to load news links for page {page_num} after {max_retries} attempts.")
        return []

    # Get News Data
    def get_news_data(self, news_url, max_retries=5):
        print(f"Fetching news data from URL: {news_url}")  # Debugging print

        for attempt in range(max_retries):
            try:
                self.driver.get(news_url)
                time.sleep(0.5)
                
                post_page_source = self.driver.page_source
                post_soup = BeautifulSoup(post_page_source, 'html.parser')

                # Extract date and time
                date_time_element = post_soup.find('em', class_='input-date')
                if date_time_element:
                    date_time_text = date_time_element.text.strip().replace('입력 ', '')
                    
                    # Convert date_time_text to datetime object
                    try:
                        news_datetime = datetime.strptime(date_time_text, "%Y.%m.%d (%H:%M)")
                    except ValueError:
                        print(f"Date format error: {date_time_text}")
                        news_datetime = None
                    
                    # If news date is outside the time range, skip or stop
                    if news_datetime is None or news_datetime > self.end_datetime:
                        print(f"News date {news_datetime} is after end_date {self.end_datetime}. Skipping.")  # Debugging print
                        return 'PASS'
                    elif news_datetime < self.start_datetime:
                        print(f"News date {news_datetime} is before start_date {self.start_datetime}. Stopping.")  # Debugging print
                        return 'STOP'
                    
                    # Extract date and time parts for further use
                    date = news_datetime.strftime("%Y-%m-%d")
                    time_of_day = news_datetime.strftime("%H:%M")

                else:
                    date = 'N/A'
                    time_of_day = 'N/A'

                title_element = post_soup.find('h4', class_='headline-title')
                title = title_element.text.strip() if title_element else 'N/A'

                body_element = post_soup.find('div', id='cont_newstext')
                body = body_element.get_text(strip=True) if body_element else 'N/A'

                news_data = {
                    "upload_date": date,
                    "upload_time": time_of_day,
                    "title": title,
                    "body": body,
                    "url": news_url
                }

                return news_data
            except Exception as e:
                print(f"Error on attempt {attempt + 1} for URL {news_url}: {e}")
                time.sleep(1)
        
        print(f"Failed to load news data for URL {news_url} after {max_retries} attempts.")
        return {}

    # Crawling News and returning DataFrame
    def crawl_news(self):
        all_news_list = []
        page_num = 1  # Starting from page 1

        while True:
            news_links = self.get_news_links(self.car_name, page_num)
            if not news_links:
                break
            
            for link in news_links:
                news_data = self.get_news_data(link)
                keep = True
                if news_data:
                    if news_data == 'PASS':
                        continue
                    elif news_data == 'STOP':
                        keep = False
                        break
                    all_news_list.append(news_data)
            if not keep:
                break
            print(f"Data for page {page_num} processed for {self.car_name}")
            
            page_num += 1
            time.sleep(0.5)

        # Return DataFrame with specified columns
        df = pd.DataFrame(all_news_list, columns=["upload_date", "upload_time", "title", "body", "url"])
        return df

    # Cleanup method to close the driver
    def close(self):
        self.driver.quit()

# Usage Example
if __name__ == "__main__":
    # 명령줄 인수를 받아서 처리
    if len(sys.argv) == 3:
        start_datetime_str = sys.argv[1]
        end_datetime_str = sys.argv[2]

        # KbsNewsCrawler 인스턴스를 생성할 때 모든 인수를 전달
        crawler = KbsNewsCrawler('그랜저', start_datetime_str, end_datetime_str)
        df = crawler.crawl_news()

        print(df)
        df.to_csv(f'kbs_{'그랜저'}_news.csv', header=True, index=False, encoding='utf-8-sig')

        crawler.close()
    else:
        print("Usage: python kbs_init_Extract.py <car-name> <start-datetime> <end-datetime>")
