'''
dcinside.com의 갤러리 내 검색에서 게시물 검색
usage:
    python3 post_links_crawler-specific_gallery.py {board_id} {query}
output:
    ./result/links_of_{query}_in_{board_id}.csv
'''

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib.parse
import pandas as pd
import sys
import os
from time import sleep

def get_semi_formatted_url(board_id, query):
    unformatted_url = "https://gall.dcinside.com/board/lists/?s_type=search_subject_memo&id={}&s_keyword={}&search_pos=-{}&page={}"
    encoded_query = urllib.parse.quote(query).replace('%', '.')
    non_paged_url = unformatted_url.format(board_id, encoded_query, "{}", "{}")
    return non_paged_url

def get_links(driver, url):
    driver.get(url)
    try:
        titles = WebDriverWait(driver, 3).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.ub-content.us-post td.gall_tit.ub-word a")),
        )
        dates = WebDriverWait(driver, 3).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.ub-content.us-post td.gall_date")),
        )
    except Exception as e:
        print(f"Elements not found: {e}")
        return []
    posts = []
    for title, date in zip(titles, dates):
        try:
            href = title.get_attribute('href')
            posts.append((title.text, href, "자동차", date.get_attribute('title')))
        except Exception as e:
            print(f"Error processing element: {e}")
            continue
    return posts

def get_links(driver, url):
    posts = []

    for _ in range(10):
        driver.get(url)
        sleep(1)  # 페이지가 로드될 시간을 기다림
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        titles = soup.select("tr.ub-content.us-post td.gall_tit.ub-word a")
        dates = soup.select("tr.ub-content.us-post td.gall_date")

        for title, date in zip(titles, dates):
            if title.text[0] == '[' and title.text[-1] == ']':
                continue

            href = title['href']
            post_url = 'https://gall.dcinside.com' + href[:href.index('&search_pos=')]
            posts.append((title.text.strip(), post_url, "자동차", date['title']))
        
        if posts:
            break
    else:
        print(f"[ERROR] 크롤링 실패 - {url}")

    return posts

def extract_total_links(board_id, query):
    semi_formatted = get_semi_formatted_url(board_id, query)
    total_links = []
    
    op = webdriver.ChromeOptions()
    op.add_argument('headless')
    op.add_argument("--log-level=3")  # 로그 레벨 설정
    driver = webdriver.Chrome(options=op)
    for pos in range(start_search_pos, end_search_pos+1, 10000):
        positioned_url = semi_formatted.format(pos, "{}")
        page = 1
        while True:
            fully_formatted_url = positioned_url.format(page)
            links = get_links(driver, fully_formatted_url)
            # 검색 결과가 없거나 마지막 페이지를 초과하는 경우 중단.
            if not links or (page > 1 and links[-1] == total_links[-1]):
                break
            print(f"[INFO] Extracted links from {fully_formatted_url}")
            total_links.extend(links)
            page += 1
    df = pd.DataFrame(total_links, columns=['Title', 'Link', 'Gallery', 'Datetime'])
    return df

def transform(df):
    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y-%m-%d %H:%M:%S')
    df['Date'] = df['Datetime'].dt.strftime('%Y-%m-%d')
    df['Time'] = df['Datetime'].dt.strftime('%H:%M')
    df = df.drop(columns=['Datetime'])
    df = df.sort_values(by=['Date', 'Time']).reset_index(drop=True)
    return df

def load(query, df):
    output_dir = './result'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_file = f"{output_dir}/links_of_{query.replace(' ', '_')}_in_{board_id}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[INFO] Output saved to {output_file}")


start_search_pos = 6180000
end_search_pos = 9700000

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("usage:\n\tpython3 post_links_crawler-specific_gallery.py {board_id} {query}")
        sys.exit(1)

    board_id = sys.argv[1]
    query = sys.argv[2]
    df = extract_total_links(board_id, query)
    df = transform(df)
    load(query, df)