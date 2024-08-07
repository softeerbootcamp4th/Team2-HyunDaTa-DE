'''
input file에 해당하는 링크에 접속해 게시글 내용, 정보 크롤링
usage:
    python3 post_contents_crawler.py {query}
output:
    ./result/contents_of_{query}.csv
'''

import sys
from time import sleep
import multiprocessing as mp
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType


################
# RANDOM PROXY #
################
def random_us_proxy():
    proxy_url = "https://www.us-proxy.org/"

    res = requests.get(proxy_url)
    soup = BeautifulSoup(res.text,'lxml')

    table = soup.find('tbody')
    rows = table.find_all('tr')
    proxy_server_list = []

    for row in rows:
        https = row.find('td', attrs = {'class':'hx'})
        if https.text == 'yes':
            ip = row.find_all('td')[0].text
            port = row.find_all('td')[1].text
            server = f"{ip}:{port}"
            proxy_server_list.append(server)

    proxy_server = random.choices(proxy_server_list)[0]
    return proxy_server




def get_post_body(soup):
    write_div = soup.select_one("div.write_div")
    body = ''

    if write_div:
        # 특정 클래스의 요소들을 제거
        excluded_classes = ['imgwrap', 'og-div']
        for excluded_class in excluded_classes:
            for element in write_div.find_all(class_=excluded_class):
                element.extract()
        
        # write_div 요소 내부의 모든 p와 div 태그의 텍스트를 가져오기
        body_elements = write_div.find_all(['p', 'div'])
        body = '\n'.join([element.get_text(separator="\n", strip=True) for element in body_elements])

    dc_app = body.endswith("- dc official App")
    if dc_app:
        body = body[:-len("- dc official App")].strip()
    
    return dc_app, body

def get_post_comments(soup):
    comment_elements = soup.select("p.usertxt.ub-word")
    
    if not comment_elements:
        return None
    
    comments = '\n'.join(
        el.text[:-len("- dc App")].strip() if el.text.endswith("- dc App") else el.text
        for el in comment_elements
    )
    
    return comments

def get_post_up_down(soup):
    try: up = soup.select_one("div.up_num_box p.up_num").text
    except AttributeError: up = None
    try: down = soup.select_one("div.down_num_box p.down_num").text
    except AttributeError: down = None
    return up, down

def process_url(driver, url):
    title = date = time = num_views = num_comments = dc_app = up = down = body = comments = None
    for _ in range(10):
        try:
            driver.get(url)
            sleep(1)  # 페이지가 로드될 시간을 기다림
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            title = soup.select_one('h3.title.ub-word span.title_subject').text
            date, time = soup.select_one('div.fl span.gall_date').text.split()
            _, num_views, _, _, _, num_comments = soup.select("div.fr")[1].text.split()
            dc_app, body = get_post_body(soup)
            if not body:
                raise Exception("본문 크롤링 실패")
            comments = get_post_comments(soup)
            if int(num_comments) > 0 and comments is None:
                raise Exception("댓글 크롤링 실패")

            up, down = get_post_up_down(soup)
            print(f"[INFO] 크롤링 완료 - {url}\n")
            break
        except Exception as e:
            print(f"[ERROR] {e} - 재시도 중 - {url}")
    else:
        print(f"[ERROR] 크롤링 실패 - {url}\n")
    
    # 공통 + dcinside
    return (title, date, time, body, comments, num_views, up, 'dcinside', url) + (num_comments, dc_app, down)


# 각 프로세스에서 사용할 작업자 함수
def worker(urls, output):
    # random proxy
    proxy_ip_port = random_us_proxy()
    proxy = Proxy()
    proxy.proxy_type = ProxyType.MANUAL
    proxy.http_proxy = proxy_ip_port
    proxy.ssl_proxy = proxy_ip_port

    # driver option
    op = webdriver.ChromeOptions()
    op.add_argument('headless')
    op.Proxy = proxy  # proxy 설정을 options 객체에 추가

    driver = webdriver.Chrome(options=op)
    results = []
    for url in urls:
        results.append(process_url(driver, url))
    driver.quit()
    output.put(results)


# Extract using multiprocessing
def extract(df, num_processes=6):
    urls = df['Link'].tolist()
    chunk_size = (len(urls) - 1) // num_processes + 1
    chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

    with mp.Manager() as manager:
        output = manager.list()
        processes = []

        for chunk in chunks:
            p = mp.Process(target=worker, args=(chunk, output))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

        results = list(output)

# return (title, date, time, body, comments, num_views, up, 'dcinside', url) + (num_comments, dc_app, down)
    post_df = pd.DataFrame(results, columns=['Title', 'Date', 'Time', 'Body', 'Comment', 'View', 'Like', 'Community', 'Url', 'Num_comments', 'DC_app', 'Dislike'])
    return post_df

# Transform
def transform(df):
    df['Date'] = pd.to_datetime(df['Date'], format='%Y.%m.%d').dt.strftime('%Y-%m-%d')
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.strftime('%H:%M')
    df['View'] = pd.to_numeric(df['View'], errors='coerce').astype('Int64')
    df['Num_comments'] = pd.to_numeric(df['Num_comments'], errors='coerce').astype('Int64')
    df['Like'] = pd.to_numeric(df['Like'], errors='coerce').astype('Int64')
    df['Dislike'] = pd.to_numeric(df['Dislike'], errors='coerce').astype('Int64')
    return df

# Load
def load(df, output_file_path):
    df.sort_values(by=['Date', 'Time'], inplace=True)
    df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    print(f"[INFO] Output saved to {output_file_path}\n")

if __name__ == "__main__":
    if len(sys.argv) == 2:
        query = sys.argv[1]
        input_file_path = f"./result/links_of_{query.replace(' ','_')}.csv"
        output_file_path = f"./result/contents_of_{query.replace(' ','_')}.csv"
    elif len(sys.argv) == 3:
        board_id = sys.argv[1]
        query = sys.argv[2]
        input_file_path = f"./result/links_of_{query.replace(' ','_')}_in_{board_id}.csv"
        output_file_path = f"./result/contents_of_{query.replace(' ','_')}_in_{board_id}.csv"
    else:
        print("usage:\n\tpython3 post_content_crawler.py {query}")
        print("usage:\n\tpython3 post_content_crawler.py {board_id} {query}")
        sys.exit(1)

    df = pd.read_csv(input_file_path)
    post_df = extract(df, num_processes=6)
    post_df = transform(post_df)
    load(post_df, output_file_path)