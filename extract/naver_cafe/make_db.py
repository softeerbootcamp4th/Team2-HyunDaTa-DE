import sqlite3
from glob import glob
import pandas as pd


def merge_csv_files(path: str = '../data/*.csv') -> pd.DataFrame:
    all_files = glob(path)
    df = pd.concat((pd.read_csv(f) for f in all_files))
    return df


def main():
    merged_df = merge_csv_files()
    merged_df['Community'] = 'naver_cafe'
    merged_df.drop("post_id", axis=1, inplace=True)
    conn = sqlite3.connect('Ioniq_naver_cafe_result.db')

    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS naver_cafe_data
    ''')

    cursor.execute('''
    CREATE TABLE naver_cafe_data (
        post_id INTEGER PRIMARY KEY AUTOINCREMENT,
        Date TEXT,
        Time TEXT,
        Title TEXT,
        Body TEXT,
        Comment TEXT,
        Community TEXT
    )
    ''')

    merged_df.to_sql('naver_cafe_data', conn, if_exists='append', index=False)
    conn.close()
    print("Data has been successfully stored in the SQLite database with post_id as the primary key.")
    return None


if __name__ == '__main__':
    main()
