import pandas as pd
import sqlite3

df = pd.read_csv('sorted.csv')
conn = sqlite3.connect('dcinside_Load_Result.db')
cursor = conn.cursor()

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

df.to_sql('dcinside_data', conn, if_exists='append', index=False)
conn.close()