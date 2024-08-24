import pandas as pd

# CSV 파일 읽기
df = pd.read_csv('bobaedream_Extract_Result/bobaedream_아이오닉_Extract_Result.csv')

# 날짜 형식을 변경하여 SQLite에서 인식할 수 있게 처리 (요일 제거)
df['Date'] = pd.to_datetime(df['Date'].str.extract(r'(\d{4}\.\d{1,2}\.\d{1,2})')[0], format='%Y.%m.%d').dt.date

# DataFrame을 정렬하여 가장 오래된 게시물이 맨 앞에 오도록 함
df = df.sort_values(by='Date').reset_index(drop=True)

# DataFrame을 CSV 파일로 저장
df.to_csv('bobaedream_Tranform_Result.csv', index=False, encoding='utf-8-sig')
