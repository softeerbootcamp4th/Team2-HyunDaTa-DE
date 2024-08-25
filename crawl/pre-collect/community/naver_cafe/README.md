## 네이버 동호회 카페 웹크롤러 사용법

### Install Requirements
- Use Poetry + Pyenv
    ```bash
    pyenv local 3.10
    poetry init
    poetry env use python3.10
    poetry add selenium pandas python-dotenv
    ```

- Use conda
    ```bash
    conda create -n cafe_crawl python=3.10 -y
    conda activate cafe_crawl
    pip install selenium pandas python-dotenv
    ```

### Add login_info.env into directory
- 네이버 로그인에 필요한 ID/PW 정보를 login_info.env에 저장합니다
    ```bash
    # login_info.env
    NAVER_ID=honggildong
    NAVER_PW=1234
    ```

### Select search options
- 검색에 사용할 옵션을 선택합니다.
- poetry로 실행
    ```bash
    poetry run python naver_cafe_crawl.py \
        --chrome_driver_path="" \
        --login_info="login_info.env" \
        --cafe_url="cafe_url" \
        --start_date="2023-01-01" \
        --end_date="2023-01-01" \
        --search_type="게시글 + 댓글" \
        --keyword="검색할 키워드" \
        --select_all="무조건 포함시킬 키워드" \
        --exclude_word="제외할 키워드" \
        --select_any="포함시킬 키워드" \
        --correct_word="" \
        --max_page_num=-1 \
        --save_path="sample.csv"
    ```
- conda로 실행
    ```bash
    python naver_cafe_crawl.py \
        --chrome_driver_path="" \
        --login_info="login_info.env" \
        --cafe_url="cafe_url" \
        --start_date="2023-01-01" \
        --end_date="2023-01-01" \
        --search_type="게시글 + 댓글" \
        --keyword="아이오닉" \
        --select_all="" \
        --exclude_word="" \
        --select_any="" \
        --correct_word="" \
        --max_page_num=-1 \
        --save_path="sample.csv"
    ```

### 
- 각 커뮤니티에서 수집한 데이터를 한번에 분석하고자 database로 변경합니다.