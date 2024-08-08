import sqlite3
import pandas as pd
import yaml

# YAML 파일에서 필터 단어 읽기
with open('bobaedream_config.yaml', 'r', encoding='utf-8') as file:
    filter_words = yaml.safe_load(file)['filter_words']

# SQLite 데이터베이스에 연결
conn = sqlite3.connect('bobaedream_Load_Result.db')

# SQL 쿼리 동적으로 생성
conditions = []
for word in filter_words:
    conditions.append(f"Title LIKE '%{word}%'")
    conditions.append(f"Body LIKE '%{word}%'")
query = f"SELECT * FROM bobaedream_data WHERE {' OR '.join(conditions)}"

# 쿼리 실행 및 결과를 데이터프레임으로 변환
df_result = pd.read_sql_query(query, conn)

# 데이터베이스 연결 종료
conn.close()

# 결과를 CSV 파일로 저장
df_result.to_csv('filtered_bobaedream_data.csv', index=False, encoding='utf-8-sig')

print("Filtered data has been successfully saved to 'filtered_bobaedream_data.csv'.")
