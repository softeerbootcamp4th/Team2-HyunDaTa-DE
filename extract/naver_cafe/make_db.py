import sqlite3
from glob import glob
import pandas as pd


def merge_csv_files(path: str = 'data/*.csv') -> pd.DataFrame:
    all_files = sorted(glob(path))
    df = pd.concat((pd.read_csv(f) for f in all_files))
    return df


def main(save_name: str):
    merged_df = merge_csv_files()
    merged_df.drop("post_id", axis=1, inplace=True)
    merged_df.to_csv(f'{save_name}.csv',
                     index=False, encoding='utf-8-sig')

    conn = sqlite3.connect(f'{save_name}.db')

    cursor = conn.cursor()
    cursor.execute(f'''
        DROP TABLE IF EXISTS {save_name}
    ''')

    cursor.execute(f'''
    CREATE TABLE {save_name} (
        post_id INTEGER PRIMARY KEY AUTOINCREMENT,
        Date TEXT,
        View TEXT,
        Like TEXT,
        Title TEXT,
        Body TEXT,
        Comment TEXT,
        Time TEXT,
        Community TEXT,
        CarName TEXT
    )
    ''')

    merged_df.to_sql(f'{save_name}', conn, if_exists='append', index=False)
    conn.close()
    print("Data has been successfully stored in the SQLite database with post_id as the primary key.")
    return None


if __name__ == '__main__':
    main(save_name="")
