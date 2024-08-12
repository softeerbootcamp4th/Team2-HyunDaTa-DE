"""
디시인사이드를 크롤링한 결과 CSV 파일을 DB 파일로 변환

Usage:
    python3 csv_to_db.py <dcinside_crawling_result_csv_file>

Example:
    python3 csv_to_db.py result/dcinside_아이오닉_2020-01-02-01-23_2023-04-05-12-34.csv

Output:
    ./result/{csv_file_name}.db
"""

import pandas as pd
import sqlite3
import sys
import os

def load_csv_to_dataframe(csv_file):
    return pd.read_csv(csv_file)

def create_database_connection(db_path):
    conn = sqlite3.connect(db_path)
    return conn, conn.cursor()

def create_table(cursor):
    cursor.execute('''
        DROP TABLE IF EXISTS dcinside_data
    ''')

    cursor.execute('''
        CREATE TABLE dcinside_data (
            post_id INTEGER PRIMARY KEY AUTOINCREMENT,
            Title TEXT,
            Date TEXT,
            Time TEXT,
            Body TEXT,
            Comment TEXT,
            View TEXT,
            Like INTEGER,
            Community TEXT,
            Url TEXT,
            Num_comments INTEGER,
            DC_app INTEGER,
            Dislike INTEGER
        )
    ''')

def insert_data_to_table(df, conn):
    df.to_sql('dcinside_data', conn, if_exists='append', index=False)

def main():
    if len(sys.argv) < 2:
        print("Usage: python script.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    
    # 입력 파일 경로에서 파일 이름만 추출하고, 확장자를 .db로 변경
    file_name = os.path.splitext(os.path.basename(csv_file))[0]
    db_path = f'result/{file_name}.db'

    df = load_csv_to_dataframe(csv_file)
    conn, cursor = create_database_connection(db_path)
    create_table(cursor)
    insert_data_to_table(df, conn)
    conn.close()

if __name__ == "__main__":
    main()
