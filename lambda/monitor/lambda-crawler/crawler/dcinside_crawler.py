"""
DC인사이드 자동차 갤러리에서 원하는 검색어의 결과 중 특정 시간 범위의 글을 크롤링

Usage:
    python3 dcinside_crawler.py <query> <start_datetime> <end_datetime>

Example:
    python3 dcinside_crawler.py 아이오닉 "2020-01-02 01:23" "2023-04-05 12:34"

Output:
    ./result/dcinside_{query}_{start_datetime}_{end_datetime}.csv
"""

import itertools
from time import sleep
import pandas as pd
import urllib.parse
import multiprocessing as mp
from bs4 import BeautifulSoup
from crawler.driver import get_driver

MAX_PAGE_ACCESS = 4
WAIT_TIME = 1 # 페이지 로드를 기다리는 시간

class DcInsideCrawler:
    def __init__(self, query, start_datetime, end_datetime, board_id='car_new1'):
        self.query = query
        self.start_datetime = start_datetime
        self.end_datetime = end_datetime
        self.board_id = board_id
    
    def start_crawling(self, num_processes=5):
        driver = get_driver()

        encoded_query = urllib.parse.quote(self.query).replace('%', '.')
        base_url = "https://gall.dcinside.com/board/lists/?s_type=search_subject_memo&id={}&s_keyword={}"
        initial_search_url = base_url.format(self.board_id, encoded_query)
        cur_search_url = self._get_start_url(driver, initial_search_url)

        total_df = pd.DataFrame(
            columns=['Date', 'Time', 'Title', 'Body', 'Comment', 'View', 'Like', 'Community', 'CarName', 'Url']
        )
        while True: # start_datetime이 될 때까지 반복
            next_search_url = self._get_next_search_url(driver, cur_search_url)
            batch_post_urls, stop_flag = self._get_batch_post_urls(driver, cur_search_url)
            
            print(f"[INFO] 배치 크롤링 시작\n")
            batch_df = self._get_batch_post_contents_df(batch_post_urls, num_processes)
            print(f"[INFO] 배치 크롤링 종료\n")

            total_df = pd.concat([total_df, batch_df])
            if len(batch_df) != 0:
                print(batch_df.iloc[-1])

            if stop_flag:
                print("[INFO] 전체 크롤링 종료\n")
                break

            cur_search_url = next_search_url

        total_df['Datetime'] = pd.to_datetime(total_df['Date'] + ' ' + total_df['Time'])
        total_df = total_df[(self.start_datetime <= total_df['Datetime']) & (total_df['Datetime'] <= self.end_datetime)]
        total_df = total_df.sort_values(by=['Datetime']).drop(['Datetime'], axis=1)
        total_df = self._transform(total_df)
        return total_df
    
    def _get_url_soup(self, driver, url):
        driver.get(url)
        sleep(WAIT_TIME)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        return soup
    
    def _get_start_url(self, driver, search_url):
        driver.get(search_url)
        sleep(WAIT_TIME)
        # 종료 날짜로 이동
        end_datetime_str = self.end_datetime.strftime("%Y-%m-%d")
        driver.execute_script(f'document.getElementById("calendarInput").value = "{end_datetime_str}";')
        driver.execute_script('document.querySelector(".btn_blue.small.fast_move_btn").click();')
        sleep(WAIT_TIME)
        return driver.current_url

    def _get_next_search_url(self, driver, search_url):
        for _ in range(MAX_PAGE_ACCESS):
            try:
                soup = self._get_url_soup(driver, search_url)
                search_next_element = soup.select_one('div.bottom_paging_box.iconpaging a.search_next')
                next_search_url = "https://gall.dcinside.com" + search_next_element.get('href')
                return next_search_url
            except Exception as e:
                print(f"[WARN] 다음 검색 링크 획득 재시도 - {e} - {search_url}")
        print(f"[ERROR] 다음 검색 링크 획득 실패 - {search_url}")
        return None
        
    def _get_batch_post_urls(self, driver, search_url):
        batch_post_urls_with_datetime = []
        stop_flag = False

        for page in itertools.count(start=1, step=1):    
            paged_search_url = search_url + f"&page={page}"
            post_urls_with_datetime = self._get_post_urls_with_datetime_from_post_list(driver, paged_search_url)
            # 검색 내용이 없거나 이전 크롤링 내용과 같으면 정지
            if not post_urls_with_datetime or (batch_post_urls_with_datetime and post_urls_with_datetime[-1][0] == batch_post_urls_with_datetime[-1][0]):
                break
            batch_post_urls_with_datetime.extend(post_urls_with_datetime)
            # start datetime보다 일찍 작성된 게시글을 불러왔으면 정지
            if batch_post_urls_with_datetime and pd.to_datetime(batch_post_urls_with_datetime[-1][1]) < self.start_datetime:
                stop_flag = True
                break
        
        batch_post_urls = [
            url
            for url,datetime in batch_post_urls_with_datetime
            if self.start_datetime <= pd.to_datetime(datetime) <= self.end_datetime
        ]

        return batch_post_urls, stop_flag
    
    ### 게시글 목록의 한 페이지에 나타난 게시글의 링크와 datetime들을 크롤링
    def _get_post_urls_with_datetime_from_post_list(self, driver, url):
        post_urls = []

        for _ in range(MAX_PAGE_ACCESS):
            try:
                prefix = "https://gall.dcinside.com"
                soup = self._get_url_soup(driver, url)
                title_elements = soup.select("tr.ub-content.us-post td.gall_tit.ub-word")
                datetime_elements = soup.select("tr.ub-content.us-post td.gall_date")
                post_urls = [
                    (prefix + title.find('a').get('href'), datetime.get('title'))
                    for title,datetime in zip(title_elements, datetime_elements)
                    if title.find('a')
                ]
            except Exception as e:
                print(f"[WARN] 게시글 목록 크롤링 재시도 - {e} - {url}\n")

            if post_urls:
                print(f"[INFO] 게시글 목록 크롤링 성공 - {url}\n")
                break
        else:
            print(f"[ERROR] 게시글 목록 크롤링 실패 - {url}\n")

        return post_urls


    ### batch url들을 멀티프로세싱을 통해 크롤링
    def _get_batch_post_contents_df(self, urls, num_processes):
        if not urls:
            return pd.DataFrame()

        post_contents = []
        if num_processes == 1: # 멀티프로세싱 안 함
            self.crawling_worker(urls, post_contents)  # worker가 url을 크롤링
        else:
            chunk_size = (len(urls) - 1) // num_processes + 1
            chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]

            with mp.Manager() as manager:
                post_contents_output = manager.list()
                processes = []  
                for chunk in chunks:
                    p = mp.Process(target=self.crawling_worker, args=(chunk, post_contents_output))
                    processes.append(p)
                    p.start()
                for p in processes:
                    p.join()
                post_contents = list(post_contents_output)

        post_contents_df = pd.DataFrame(
            post_contents,
            columns=['Date', 'Time', 'Title', 'Body', 'Comment', 'View', 'Like', 'Community', 'CarName', 'Url']
        )
        return post_contents_df

    def crawling_worker(self, urls, post_contents):
        driver = get_driver()
        for url in urls:
            post_content = self._get_single_post_content(driver, url)
            post_contents.append(post_content)
        driver.quit()

    ### 하나의 게시글에서 내용 크롤링
    def _get_single_post_content(self, driver, url):
        # 본문 크롤링
        def _get_post_body(soup):
            write_div = soup.select_one("div.write_div")
            body = ''
            if write_div is not None:
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

        # 댓글 크롤링
        def _get_post_comments(soup):
            comment_elements = soup.select("p.usertxt.ub-word")
            if not comment_elements:
                return None
            comments = '\n'.join(
                el.text[:-len("- dc App")].strip() if el.text.endswith("- dc App") else el.text
                for el in comment_elements
            )
            return comments

        # 추천/비추천 크롤링
        def _get_post_up_down(soup):
            try: up = soup.select_one("div.up_num_box p.up_num").text
            except AttributeError: up = None
            try: down = soup.select_one("div.down_num_box p.down_num").text
            except AttributeError: down = None
            return up, down
        
        ##########################################################################################
        title = date = time = views = num_comments = dc_app = like = dislike = body = comments = None
        for _ in range(MAX_PAGE_ACCESS):
            try:
                soup = self._get_url_soup(driver, url)
                title = soup.select_one('h3.title.ub-word span.title_subject').text
                date, time = soup.select_one('div.fl span.gall_date').text.split()
                _, views, _, _, _, num_comments = soup.select("div.fr")[1].text.split()
                dc_app, body = _get_post_body(soup)
                comments = _get_post_comments(soup)
                like, dislike = _get_post_up_down(soup)
                if not body:
                    raise Exception("본문 크롤링 실패")
                if int(num_comments) > 0 and comments is None:
                    raise Exception("댓글 크롤링 실패")
                if like is None: # up만 확인하는 이유: 가끔 비추가 아예 없는 글이 있음
                    raise Exception("추천 수 크롤링 실패")

                print(f"[INFO] 게시글 크롤링 완료 - {url}\n")
                break
            except Exception as e:
                print(f"[WARN] {e} - 게시글 크롤링 재시도 - {url}\n")
        else:
            print(f"[ERROR] 게시글 크롤링 실패 - {url}\n")
        
        # 공통 + dcinside
        return (date, time, title, body, comments, views, like, 'dcinside', self.query, url) # + (num_comments, dc_app, dislike)

    def _transform(self, df):
        df['Date'] = pd.to_datetime(df['Date'], format='%Y.%m.%d').dt.strftime('%Y-%m-%d')
        df['Time'] = pd.to_datetime(df['Time'], format='%H:%M:%S').dt.strftime('%H:%M')
        df['View'] = pd.to_numeric(df['View'], errors='coerce').astype('Int64')
        df['Like'] = pd.to_numeric(df['Like'], errors='coerce').astype('Int64')
        # df['NumComments'] = pd.to_numeric(df['NumComments'], errors='coerce').astype('Int64')
        # df['Dislike'] = pd.to_numeric(df['Dislike'], errors='coerce').astype('Int64')
        print(df.head(1))
        return df
