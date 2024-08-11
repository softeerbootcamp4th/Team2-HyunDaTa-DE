# 디시인사이드 웹크롤러
디시인사이드 자동차 갤러리의 게시물 내용을 크롤링하여 결과를 CSV 파일과 데이터베이스로 저장

## Install Requirements
- beautifulsoup4 >= 4.12.3
- pandas >= 2.2.2
- requests >= 2.32.3
- selenium >= 4.23.1

## Files
- **dcinside_crawler.py**
    - DC인사이드 자동차 갤러리에서 원하는 검색어의 결과 중 특정 시간 범위의 글을 크롤링
    - Usage: `python3 dcinside_crawler.py <query> <start_datetime> <end_datetime>`
    - Example: `python3 dcinside_crawler.py 아이오닉 "2020-01-02 01:23" "2023-04-05 12:34"`
    - Output: `./result/dcinside_{query}_{start_datetime}_{end_datetime}.csv`
- **dcinside_monthly_crawler.sh**
    - 크롤러를 주어진 기간 동안 월 단위로 실행하는 배치 스크립트
    - 전체 크롤링이 마무리 되면 크롤링 된 결과를 merged 디렉터리에 하나로 병합
    - 크롤링의 소요 시간이 길어 안정성을 보장하고자 할 때 사용
    - Run command: `./batch_crawler-monthly.sh <query> <start_datetime> <end_datetime>`
- **csv_to_db.py**
    - 크롤링한 데이터를 CSV 파일에서 데이터베이스로 변환
    - Run command: `python3 csv_to_db.py <dcinside_crawling_result_csv_file>`
    - Example: `python3 csv_to_db.py result/dcinside_아이오닉_2020-01-02-01-23_2023-04-05-12-34.csv`
    - Output: result 디렉토리에 동일한 이름의 .db 파일이 생성됨

## Notes
Datetime의 형식은 `yyyy-MM-dd hh:mm`을 권장합니다.