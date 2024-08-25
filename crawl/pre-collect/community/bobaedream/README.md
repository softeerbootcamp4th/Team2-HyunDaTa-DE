# Bodaedream ETL Process and Plot

## 1. bobaedream_Extract_Multi.py, bobaedream_Extract_Single.py
* Multiprocessing을 사용한 Extract와 사용하지 않은 Extract 파일
* 원하는 기간에 대한 bobaedream Community의 Keyword 게시물을 Extract한다.

```bash
python bobaedream_Extract_Multi.py {start-date} {end-date}
```

## 2. bobaedream Community 전처리
* Date format이 SQLite와 같이 않아서 전처리 해주었다.
```bash
python bobaedream_Transform.py
```

## 3. Transform 후의 csv Data를 SQLite에 Load
* Load할 때, post_id를 PK로 지정하고 AUTOINCREMENT하게 설정하였다.
```bash
python bobaedream_load.py
```

## 4. filtering할 word를 포함할 Data SELECT
* Title, Body에 누수 관련 word들을 포함한 게시물들을 SELECT하여 csv로 저장
```bash
python bobaedream_select.py
```

## 5. Filtering 후에 Plot 및 Hot Issue Trigger Check
* filtering_bobaedream.ipynb