import os
import time
from datetime import datetime, timedelta
import pandas as pd
from dotenv import load_dotenv
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from crawler.driver import get_driver


class NaverCafeCralwer:
    def __init__(self):
        self.__naver_crawl_config = {
            "casper": {
                "search_url": "https://cafe.naver.com/cafe2078",
                "exclude_word": "출석 출첵 가입 등업신청 퇴근",
                "search_keyword": "캐스퍼",
            },
            "granduer": {
                "search_url": "https://cafe.naver.com/bestcm",
                "exclude_word": "출석 출첵 가입 등업신청 퇴근",
                "search_keyword": "그랜저",
            },
            "palisade": {
                "search_url": "https://cafe.naver.com/naworl",
                "exclude_word": "출석 출첵 가입 등업신청 퇴근",
                "search_keyword": "팰리세이드",
            },
            "ioniq": {
                "search_url": "https://cafe.naver.com/cafeclip",
                "exclude_word": "출석 출첵 가입 등업신청 퇴근",
                "search_keyword": "아이오닉",
            },
            "avante": {
                "search_url": None,
                "exclude_word": "출석 출첵 가입 등업신청 퇴근",
                "search_keyword": "아반떼",
            },
            "genesis": {
                "search_url": None,
                "exclude_word": "출석 출첵 가입 등업신청 퇴근",
                "search_keyword": "제네시스",
            },
        }
        self.current_crawl_option = None
        self.driver = get_driver()

    def get_crawl_option(self, car_name: str):
        try:
            return self.__naver_crawl_config[car_name]
        except:
            raise ValueError("해당 차량에 대한 크롤링 설정이 존재하지 않습니다")

    def set_current_crawl_option(self, car_name: str) -> None:
        self.car_name = car_name
        self.current_crawl_option = self.get_crawl_option(car_name)

    def start_crawling(self, end_datetime: datetime) -> pd.DataFrame:
        self.naver_login()
        self.search_keyword()
        extract_data = self.extract(end_datetime)
        transform_data = self.transform(
            extract_data, self.current_crawl_option["search_keyword"])
        return transform_data

    def naver_login(self):
        """Login to Naver

        Args:
            login_info (str): Path of login information file (login_info.env)

        Returns:
            None
        """
        self.driver.get("https://nid.naver.com/nidlogin.login")
        load_dotenv(dotenv_path='crawler/login_info.env')
        login_id = os.getenv(self.car_name+'_id')
        login_pw = os.getenv(self.car_name+'_pw')
        if login_id is None or login_pw is None:
            raise Exception("로그인 정보를 찾을 수 없습니다.")
            
        # 로그인 정보 인력 (headless에서도 작동)
        self.driver.execute_script(
            f"document.querySelector('input[id=\"id\"]').setAttribute('value', '{login_id}')"
        )
        time.sleep(1)
        self.driver.execute_script(
            f"document.querySelector('input[id=\"pw\"]').setAttribute('value', '{login_pw}')"
        )
        time.sleep(1)

        login_button = self.driver.find_element(By.ID, "log.login")
        login_button.click()
        time.sleep(1)

    def search_keyword(self):
        """Search the keyword in Naver Cafe

        Args:
            keyword_dict (dict): Keyword dictionary

        Returns:
            None
        """
        # 접속 및 검색 키워드 입력
        self.driver.get(self.current_crawl_option["search_url"])
        search_box = self.driver.find_element(By.NAME, "query")
        search_box.send_keys(self.current_crawl_option["search_keyword"])
        search_box.send_keys(Keys.RETURN)
        time.sleep(2)

        self.driver.switch_to.frame("cafe_main")
        detail_infos = ["", self.current_crawl_option['exclude_word'], "", ""]
        self.driver.find_element(By.ID, 'detailSearchBtn').click()
        time.sleep(1)

        srch_details = self.driver.find_elements(By.ID, 'srch_detail')
        srch_details = self.driver.find_elements(By.CLASS_NAME, '_detail_input')
        for srch_detail, detail_info in zip(srch_details, detail_infos):
            srch_detail.clear()
            srch_detail.send_keys(detail_info)

        self.driver.find_elements(By.CLASS_NAME, 'btn-search-green')[0].click()
        time.sleep(1)

    def extract(self, end_datetime: datetime) -> pd.DataFrame:
        """Extract the data from Naver Cafe

        Args:
            max_page_num (int): Maximum page number

        Raises:
            Exception: Maximum page number

        Returns:
            pd.DataFrame: Extradted data
        """
        data = {
            'date': [],
            'view': [],
            'like': [],
            'title': [],
            'content': [],
            'comments': [],
            'url': []
        }
        max_page_num = 10000
        page_num = 1
        run_flag = True
        while run_flag and page_num <= max_page_num:
            try:
                print(f"페이지 번호: {page_num}")
                # 게시물 목록에서 링크 추출 및 정보 크롤링
                articles = self.driver.find_elements(By.CSS_SELECTOR, "a.article")
                for article in articles:
                    link = article.get_attribute("href")
                    if link:
                        print(f"게시물 링크: {link}")
                        self.driver.execute_script(
                            "window.open(arguments[0]);", link)
                        self.driver.switch_to.window(self.driver.window_handles[-1])
                        flag, sub_data = self.extract_post_info(end_datetime)

                        if flag == "False":
                            run_flag = False
                            break
                        if flag == 'True':
                            data['date'].append(sub_data['date'])
                            data['view'].append(sub_data['view'])
                            data['like'].append(sub_data['like'])
                            data['title'].append(sub_data['title'])
                            data['content'].append(sub_data['content'])
                            data['comments'].append(sub_data['comments'])
                            data['url'].append(sub_data['url'])

                        self.driver.close()
                        self.driver.switch_to.window(self.driver.window_handles[0])
                        self.driver.switch_to.frame("cafe_main")

            except:
                print("게시물 목록이 더 이상 존재하지 않습니다")
                pass

            try:
                page_num += 1
                next_page_links = self.driver.find_elements(
                    By.CSS_SELECTOR, "#main-area > div.prev-next > a")
                for link in next_page_links:
                    if link.text == str(page_num):
                        link.click()
                        time.sleep(1)
                        break
                else:
                    raise Exception()

            except Exception as e:
                print(f"마지막 페이지입니다.{e}")
                break

            if page_num % 10 == 0:
                print(f"{page_num} 페이지까지 크롤링 완료")
                try:
                    self.driver.find_element(By.CLASS_NAME, 'pgR').click()
                except:
                    break

        print("크롤링 완료")

        df = pd.DataFrame(
            data, columns=[
                'date', 'view', 'like',
                'title', 'content', 'comments', 'url'
            ])
        return df

    def extract_post_info(self, end_datetime: datetime) -> tuple:
        """Extract the post information

        Args:
            start_date (datetime): start datetime to crawl
        Returns:
            bool: True if success, False if fail
            dict: Extracted data
        """
        time.sleep(2)
        try:
            self.driver.switch_to.frame("cafe_main")

            cont_url = self.driver.find_element(
                By.XPATH, '//*[@id="spiButton"]').get_attribute('data-url')
            cont_num = cont_url.split("/")[-1]
            print(f"게시물 번호: {cont_num}")
            cont_date = "" if self.driver.find_element(
                By.CLASS_NAME, 'date').text == "" else self.driver.find_element(By.CLASS_NAME, 'date').text
            cont_title = "" if self.driver.find_element(
                By.CLASS_NAME, 'title_text').text == "" else \
                self.driver.find_element(By.CLASS_NAME, 'title_text').text
            cont_view = "0" if self.driver.find_element(
                By.CLASS_NAME, 'count').text == "" else \
                self.driver.find_element(By.CLASS_NAME, 'count').text

            like_cnt = self.driver.find_element(By.CLASS_NAME, 'like_article')
            cont_like = "0"
            try:
                cont_like = like_cnt.find_element(
                    By.CSS_SELECTOR, 'em.u_cnt._count').text
            except:
                pass
            content_texts = ""

            try:
                content_container = self.driver.find_element(
                    By.CLASS_NAME, 'se-main-container')
                content_div_tags = content_container.find_elements(
                    By.TAG_NAME, 'div')
                for cont_value in content_div_tags:
                    # text
                    if cont_value.get_attribute("class") == "se-component se-text se-l-default":
                        cont_text = cont_value.text
                        content_texts += cont_text+"\n"
                    else:
                        continue
            except:
                print("게시물 내용이 존재하지 않습니다")

            formal = ""
            comments = []

            try:
                comment_ul = self.driver.find_element(By.CLASS_NAME, 'comment_list')
                lines = comment_ul.find_elements(By.TAG_NAME, 'li')
                for line in lines:
                    cls_name = line.get_attribute("class")
                    if cls_name == "CommentItem CommentItem--reply":
                        formal += "\n" + line.text
                    elif cls_name == "CommentItem":
                        comments.append(formal)
                        formal = line.text
                    else:
                        break
            except:
                print("댓글이 존재하지 않습니다")

            datetime_object = datetime.strptime(cont_date, "%Y.%m.%d. %H:%M")

            print(datetime_object, end_datetime)
            if abs(end_datetime - datetime_object) < timedelta(hours=1):
                return "True", {
                    'url': cont_url,
                    'date': cont_date,
                    'title': cont_title,
                    'view': cont_view,
                    'like': cont_like,
                    'content': content_texts,
                    'comments': comments
                }
            else:
                return "False", {}

        except Exception as e:
            print(f"게시물 정보를 추출할 수 없습니다: {e}")
            return "No Data", {}

    def transform(self, df: pd.DataFrame, keyword: str) -> pd.DataFrame:
        """Transform the extracted data to DataFrame
        Args:
            df (pd.DataFrame): Extracted data from Naver Cafe

        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        try:
            df['datetime'] = pd.to_datetime(
                df['date'], format='%Y.%m.%d. %H:%M')

            # 날짜와 시간을 각각의 열로 분리
            df['date'] = df['datetime'].dt.date
            df['time'] = df['datetime'].dt.time
            df['view'] = df['view'].str.replace("조회 ", "")
            df['Community'] = "naver_cafe"
            df['CarName'] = keyword
            df.rename(
                columns={
                    'num': 'post_id', 'date': 'Date', 'title': 'Title', 'view': 'View',
                    'like': 'Like', 'time': 'Time', 'content': 'Body', 'comments': 'Comment',
                    'url': 'Url'
                },
                inplace=True
            )
            df.drop(['datetime'], axis=1, inplace=True)
            return df[['Date', 'Time', 'Title', 'Body', 'Comment', 'View', 'Like', 'Community', 'CarName', 'Url']]
        except:
            print("추출한 게시물이 존재하지 않습니다")
            return pd.DataFrame()