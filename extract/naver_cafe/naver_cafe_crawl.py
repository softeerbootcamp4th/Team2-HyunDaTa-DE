import os
import time
import argparse

import pandas as pd
from dotenv import load_dotenv

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


def naver_login(driver: webdriver.Chrome, login_info: str):
    """Login to Naver

    Args:
        driver (webdriver.Chrome): Chrome driver
        login_info (str): Path of login information file (login_info.env)

    Returns:
        None
    """
    driver.get("https://nid.naver.com/nidlogin.login")
    load_dotenv(dotenv_path=login_info, verbose=True)
    login_id = os.getenv("NAVER_ID")
    login_pw = os.getenv("NAVER_PW")

    # 로그인 정보 인력 (headless에서도 작동)
    driver.execute_script(
        f"document.querySelector('input[id=\"id\"]').setAttribute('value', '{login_id}')"
    )
    time.sleep(1)
    driver.execute_script(
        f"document.querySelector('input[id=\"pw\"]').setAttribute('value', '{login_pw}')"
    )
    time.sleep(1)

    login_button = driver.find_element(By.ID, "log.login")
    login_button.click()
    time.sleep(1)


def search_keyword(driver: webdriver.Chrome, keyword_dict: dict):
    """Search the keyword in Naver Cafe

    Args:
        driver (webdriver.Chrome): Chrome driver
        keyword_dict (dict): Keyword dictionary

    Returns:
        None
    """

    # 접속 및 검색 키워드 입력
    driver.get(keyword_dict["cafe_url"])
    search_box = driver.find_element(By.NAME, "query")
    search_box.send_keys(keyword_dict["keyword"])
    search_box.send_keys(Keys.RETURN)
    time.sleep(2)

    driver.switch_to.frame("cafe_main")

    # 날짜 입력
    if keyword_dict["start_date"] == "" and keyword_dict["end_date"] == "":
        pass
    else:
        driver.find_element(By.ID, 'currentSearchDateTop').click()
        start_date = driver.find_element(By.ID, 'input_1_top')
        start_date.clear()
        start_date.send_keys(keyword_dict["start_date"])
        time.sleep(1)

        end_date = driver.find_element(By.ID, 'input_2_top')
        end_date.clear()
        end_date.send_keys(keyword_dict["end_date"])
        time.sleep(1)

        driver.find_element(By.ID, 'btn_set_top').click()
        time.sleep(1)

    # 검색 조건 선택
    if keyword_dict["search_type"] == "":
        pass
    else:
        # 검색 조건 선택
        driver.find_element(By.ID, 'currentSearchByTop').click()
        ul_element = driver.find_element(By.ID, "sl_general")
        li_elements = ul_element.find_elements(By.TAG_NAME, "li")

        for li in li_elements:
            a_tag = li.find_element(By.TAG_NAME, "a")
            if a_tag.text == keyword_dict["search_type"]:
                a_tag.click()
                break

    detail_infos = [keyword_dict["select_all"], keyword_dict["exclude_word"],
                    keyword_dict["select_any"], keyword_dict["correct_word"]]

    if all(detail_info == "" for detail_info in detail_infos):
        pass
    else:
        # 상세 검색 조건 입력
        driver.find_element(By.ID, 'detailSearchBtn').click()
        time.sleep(1)

        srch_details = driver.find_elements(By.ID, 'srch_detail')
        srch_details = driver.find_elements(By.CLASS_NAME, '_detail_input')
        for srch_detail, detail_info in zip(srch_details, detail_infos):
            srch_detail.clear()
            srch_detail.send_keys(detail_info)

    driver.find_elements(By.CLASS_NAME, 'btn-search-green')[0].click()
    time.sleep(1)


def extract(driver: webdriver.Chrome, max_page_num: int) -> pd.DataFrame:
    """Extract the data from Naver Cafe

    Args:
        driver (webdriver.Chrome): Chrome driver
        max_page_num (int): Maximum page number

    Raises:
        Exception: Maximum page number

    Returns:
        pd.DataFrame: Extradted data
    """
    data = {
        'num': [],
        'date': [],
        'view': [],
        'like': [],
        'title': [],
        'content': [],
        'comments': []
    }
    max_page_num = 10000 if max_page_num == -1 else max_page_num
    page_num = 1
    while page_num <= max_page_num:
        try:
            print(f"페이지 번호: {page_num}")
            # 게시물 목록에서 링크 추출 및 정보 크롤링
            articles = driver.find_elements(By.CSS_SELECTOR, "a.article")
            for article in articles:
                link = article.get_attribute("href")
                if link:
                    print(f"게시물 링크: {link}")
                    driver.execute_script("window.open(arguments[0]);", link)
                    driver.switch_to.window(driver.window_handles[-1])
                    flag, sub_data = extract_post_info(driver)

                    if flag:
                        data['num'].append(sub_data['num'])
                        data['date'].append(sub_data['date'])
                        data['view'].append(sub_data['view'])
                        data['like'].append(sub_data['like'])
                        data['title'].append(sub_data['title'])
                        data['content'].append(sub_data['content'])
                        data['comments'].append(sub_data['comments'])

                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    driver.switch_to.frame("cafe_main")

        except:
            print("게시물 목록이 더 이상 존재하지 않습니다")
            pass

        try:
            page_num += 1
            next_page_links = driver.find_elements(
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
                driver.find_element(By.CLASS_NAME, 'pgR').click()
            except:
                break

    print("크롤링 완료")

    df = pd.DataFrame(
        data, columns=[
            'num', 'date', 'view', 'like',
            'title', 'content', 'comments'
        ])
    return df


def extract_post_info(driver: webdriver.Chrome):
    """Extract the post information

    Args:
        driver (webdriver.Chrome): Chrome driver

    Returns:
        bool: True if success, False if fail
        dict: Extracted data
    """
    time.sleep(2)
    try:
        driver.switch_to.frame("cafe_main")
        cont_url = driver.find_element(
            By.XPATH, '//*[@id="spiButton"]').get_attribute('data-url')
        cont_num = cont_url.split("/")[-1]
        print(f"게시물 번호: {cont_num}")
        cont_date = "" if driver.find_element(
            By.CLASS_NAME, 'date').text == "" else driver.find_element(By.CLASS_NAME, 'date').text
        cont_title = "" if driver.find_element(
            By.CLASS_NAME, 'title_text').text == "" else \
            driver.find_element(By.CLASS_NAME, 'title_text').text
        cont_view = "0" if driver.find_element(
            By.CLASS_NAME, 'count').text == "" else \
            driver.find_element(By.CLASS_NAME, 'count').text

        like_cnt = driver.find_element(By.CLASS_NAME, 'like_article')
        cont_like = "0"
        try:
            cont_like = like_cnt.find_element(
                By.CSS_SELECTOR, 'em.u_cnt._count').text
        except:
            pass
        content_texts = ""

        try:
            content_container = driver.find_element(
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

                # if use image
                # elif cont_value.get_attribute("class") == "se-component se-image se-l-default":
                # cont_url = cont_value.find_element(
                #     By.TAG_NAME, 'img').get_attribute('src')
                # content_img_urls.append(cont_url)
        except:
            print("게시물 내용이 존재하지 않습니다")

        formal = ""
        comments = []

        try:
            comment_ul = driver.find_element(By.CLASS_NAME, 'comment_list')
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

        comments.append(formal)
        return True, {
            'num': cont_num,
            'date': cont_date,
            'title': cont_title,
            'view': cont_view,
            'like': cont_like,
            'content': content_texts,
            'comments': comments
        }

    except Exception as e:
        print(f"게시물 정보를 추출할 수 없습니다: {e}")
        return False, {}


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the extracted data to DataFrame
    Args:
        df (pd.DataFrame): Extracted data from Naver Cafe

    Returns:
        pd.DataFrame: Transformed DataFrame
    """
    df['num'] = [i for i in range(1, len(df['num'])+1)]
    df['datetime'] = pd.to_datetime(df['date'], format='%Y.%m.%d. %H:%M')

    # 날짜와 시간을 각각의 열로 분리
    df['date'] = df['datetime'].dt.date
    df['time'] = df['datetime'].dt.time
    df['view'] = df['view'].str.replace("조회 ", "")
    df.rename(
        columns={
            'num': 'post_id', 'date': 'Date', 'title': 'Title', 'view': 'View',
            'like': 'Like', 'time': 'Time', 'content': 'Body', 'comments': 'Comment'
        },
        inplace=True
    )
    df.drop(['datetime'], axis=1, inplace=True)

    return df


def load(df: pd.DataFrame, datetime: str, save_path: str):
    """Load the transformed data to csv file

    Args:
        df (pd.DataFrame): Transformed DataFrame

    Returns:
        None

    Examples:
        | post_id | Date       | Time     | Title | Body | Comment |
        | ------- | ---------- | -------- | ----- | ---- | ------- |
        | 1       | 2023-08-01 | 12:00:00 | 제목1  | 내용  | 댓글1    |
    """
    if not os.path.exists('data/'):
        os.makedirs('data/')
    df.to_csv(f"data/{datetime}_{save_path}", index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--chrome_driver_path", type=str,
                        default="./chromedriver", help="Chrome driver path")
    parser.add_argument("--login_info", type=str, default="login_info.env",
                        help="네이버 로그인 (ID, PW) 정보가 담긴 env 파일 경로")
    parser.add_argument("--cafe_url", type=str, default="https://cafe.naver.com/cafeclip/",
                        help="Cafe URL")
    parser.add_argument("--start_date", type=str, default="2023-08-05",
                        help="탐색 시작 날짜")
    parser.add_argument("--end_date", type=str, default="2024-08-05",
                        help="탐색 종료 날짜")
    parser.add_argument("--search_type", type=str, default="게시글 + 댓글",
                        help="Search type: 제목만 or 게시글 + 댓글")
    parser.add_argument("--keyword", type=str, default="아이오닉",
                        help="Keyword for searching")
    parser.add_argument("--select_all", type=str, default="",
                        help="상세 검색 중 '다음 단어 모두 포함' 항목")
    parser.add_argument("--exclude_word", type=str, default="출석 출첵 가입 등업신청 퇴근",
                        help="상세 검색 중 '다음 단어 제외' 항목")
    parser.add_argument("--select_any", type=str, default="",
                        help="상세 검색 중 '다음 단어 중 1개 이상 포함' 항목")
    parser.add_argument("--correct_word", type=str, default="",
                        help="상세 검색 중 '정확히 일치' 항목")
    parser.add_argument("--max_page_num", type=int, default=-1,
                        help="추출할 서브 페이지 수")
    parser.add_argument("--save_path", type=str, default="ioniq_naver_cafe.csv",
                        help="저장 경로")
    args = parser.parse_args()

    # 네이버 로그인
    options = webdriver.ChromeOptions()
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Headless 모드
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument(
        "--disable-web-security")  # Disable web security
    chrome_options.add_argument("--allow-running-insecure-content")
    chrome_options.add_argument("--disable-extensions")

    # 크롬 드라이버 경로 설정
    service = Service(args.chrome_driver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)

    # driver = webdriver.Chrome(options=options)
    naver_login(driver, args.login_info)

    # 검색 키워드 입력
    keyword_dict = {
        "cafe_url": args.cafe_url,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "keyword": args.keyword,
        "search_type": args.search_type,
        "select_all": args.select_all,
        "exclude_word": args.exclude_word,
        "select_any": args.select_any,
        "correct_word": args.correct_word,
    }
    search_keyword(driver, keyword_dict)
    extract_data = extract(driver, args.max_page_num)
    transform_data = transform(extract_data)
    load(transform_data, args.start_date, args.save_path)

    # 드라이버 종료
    driver.quit()
