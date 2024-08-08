# 디시인사이드 웹크롤러
디시인사이드의 게시물 내용을 크롤링하여 결과를 CSV 파일과 데이터베이스로 저장

### Install Requirements
```bash
python3 -m venv venv
source venv/bin/activate
pip3 install pandas requests bs4 selenium lxml
```

### Files
- **csv_to_db.py**
    - 크롤링한 데이터를 CSV 파일에서 데이터베이스로 변환하는 스크립트
    - run command: `python3 csv_to_db`
- **post_contents_crawler.py**
    - 저장된 링크로부터 디시인사이드 게시물의 내용을 크롤링하는 스크립트
    - run command: `python3 post_contents_crawler.py {검색어}`
    - run command: `python3 post_contents_crawler.py {갤러리 ID} {검색어}`
- **post_contents_crawler-monthly.py**
    - 저장된 링크로부터 디시인사이드 게시물의 내용을 크롤링하는 스크립트
    - URL을 월별로 분할 작업해 temp_result 디렉터리에 저장 후 마지막에 병합
    - run command: `python3 post_contents_crawler-monthly {검색어}`
    - run command: `python3 post_contents_crawler-monthly {갤러리 ID} {검색어}`
- **post_links_crawler.py**
    - 디시인사이드의 게시물 링크를 전체 갤러리에서 크롤링하는 스크립트
    - run command: `python3 post_links_crawler.py {검색어}`
- **post_links_crawler-specific_gallery.py**
    - 디시인사이드의 게시물 링크를 특정 갤러리에서 크롤링하는 스크립트
    - run command: `python3 post_links_crawler.py {갤러리 ID} {검색어}`

### 사용 방법
1. 크롤러 실행:
   - `post_links_crawler.py` 또는 `post_links_crawler-specific_gallery.py`를 실행하여 게시물 링크를 크롤링
   - `post_contents_crawler.py` 또는 `post_contents_crawler-monthly.py`를 실행하여 게시물 내용을 크롤링
2. 데이터베이스 변환:
   - `csv_to_db.py`를 실행하여 CSV 파일에 저장된 데이터를 데이터베이스로 변환

