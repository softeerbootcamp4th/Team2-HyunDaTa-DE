import sys
import time
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By

from crawler.driver import get_driver

class MbcNewsCrawler:
    def __init__(self, car_name, start_datetime, end_datetime):
        self.car_name = car_name
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.driver = get_driver()

    # Get Each News URL
    def get_news_links(self, query, page_num, max_retries=5):
        url = f"https://imnews.imbc.com/more/search/?search_kwd={query}#page={page_num}"
        print(f"Fetching news links from URL: {url}")

        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                time.sleep(0.5)

                news_items = self.driver.find_elements(By.CSS_SELECTOR, 'li.item')
                news_links = [item.find_element(By.TAG_NAME, 'a').get_attribute('href') for item in news_items]
                print(f"Found {len(news_links)} news links on page {page_num}.")
                return news_links
            except Exception as e:
                print(f"Error on attempt {attempt + 1} for page {page_num}: {e}")
                time.sleep(1)

        print(f"Failed to load news links for page {page_num} after {max_retries} attempts.")
        return []

    # Get News Data
    def get_news_data(self, news_url, max_retries=5):
        for attempt in range(max_retries):
            try:
                self.driver.get(news_url)
                time.sleep(1)

                post_page_source = self.driver.page_source
                post_soup = BeautifulSoup(post_page_source, 'html.parser')

                # Extract Date and Time
                date_time_element = post_soup.find('span', class_='input')
                if date_time_element:
                    date_time_text = date_time_element.text.strip()
                    date_time_parts = date_time_text.split()
                    date = date_time_parts[1]
                    time_of_day = date_time_parts[2]

                    news_datetime = datetime.strptime(date + ' ' + time_of_day, "%Y-%m-%d %H:%M")
                    if news_datetime > self.end_datetime:
                        return 'PASS'
                    elif news_datetime < self.start_datetime:
                        return 'STOP'
                else:
                    date = 'N/A'
                    time_of_day = 'N/A'

                title_element = post_soup.find('h2', class_='art_title')
                title = title_element.text.strip() if title_element else 'N/A'

                body_element = post_soup.find('div', class_='news_txt')
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
        page_num = 0  # Starting from page 0

        while True:
            news_links = self.get_news_links(self.car_name, page_num)
            if not news_links:
                break
            
            for link in news_links:
                print(f"Processing link: {link}")
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
            print(f"Data for page {page_num} processed for {self.car_name}.")
            
            page_num += 1
            time.sleep(0.5)

        if not all_news_list:
            print(f"No news articles found for the query: {self.car_name} in the specified date range.")
        else:
            print(f"Total {len(all_news_list)} news articles found.")

        # Return DataFrame with specified columns
        df = pd.DataFrame(all_news_list, columns=["upload_date", "upload_time", "title", "body", "url"])
        return df

    # Cleanup method to close the driver
    def close(self):
        self.driver.quit()

# Usage Example
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python mbc_news_crawler_with_date_filter.py <start-datetime> <end-datetime>")
        sys.exit(1)

    start_datetime_str = sys.argv[1]
    end_datetime_str = sys.argv[2]

    crawler = MbcNewsCrawler('그랜저', start_datetime_str, end_datetime_str)
    df = crawler.crawl_news()

    if not df.empty:
        output_csv = f"mbc_{'그랜저'}_Extract_Result.csv"
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"Data extraction and CSV file creation completed for {'그랜저'}.")
    else:
        print(f"No data to save for {'그랜저'}.")

    crawler.close()
