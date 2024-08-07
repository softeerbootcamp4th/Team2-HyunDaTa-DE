#!/usr/bin/env python

'''
dcinside.com의 통합 검색에서 게시물을 최신순으로 검색
여기서 크롤링되는 게시글의 날짜는 게시글 내부에서 표시되는 날짜와 다름.

usage:
    python3 post_links_crawler.py {query}

output:
    ./result/links_of_{query}.csv
'''

import requests
from bs4 import BeautifulSoup as bs
import urllib.parse
import pandas as pd
import sys
import os

def get_non_paged_url(query):
    base_url = "https://search.dcinside.com/post/p/%d/q/"
    
    encoded_query = urllib.parse.quote(query).replace('%', '.')
    non_paged_url = base_url + encoded_query
    return non_paged_url

def get_links(url):
    response = requests.get(url)
    soup = bs(response.text, 'html.parser')
    title_soup = soup.select('ul.sch_result_list li a')[::2]
    gallery_soup = soup.select('ul.sch_result_list li p a')
    date_soup = soup.select('ul.sch_result_list li p span')

    # title, link, gallery, datetime
    posts = [
        (title_soup[i].text, title_soup[i]['href'], gallery_soup[i].text, date_soup[i].text)
        for i in range(len(title_soup))
    ]
    return posts

def extract_total_links(query):
    non_paged_url = get_non_paged_url(query)
    total_links = []
    page = 1
    while True: 
        paged_url = non_paged_url % page
        links = get_links(paged_url)

        # 검색 결과가 없거나 마지막 페이지를 초과하는 경우 중단.
        if not links or (page > 1 and links[-1] == total_links[-1]):
            break
        print(f"[INFO] Extracted from {paged_url}")
        total_links.extend(links)
        page += 1
    
    df = pd.DataFrame(total_links, columns=['Title', 'Link', 'Gallery', 'Datetime'])
    return df



def transform(df):
    df['Datetime'] = pd.to_datetime(df['Datetime'], format='%Y.%m.%d %H:%M')
    
    df['Date'] = df['Datetime'].dt.strftime('%Y-%m-%d')
    df['Time'] = df['Datetime'].dt.strftime('%H:%M')
    
    df = df.drop(columns=['Datetime'])
    df = df.sort_values(by=['Date', 'Time']).reset_index(drop=True)
    return df

def load(query, df):
    output_dir = './result'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    output_file = f"{output_dir}/links_of_{query.replace(' ', '_')}.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"[INFO] Output saved to {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage:\n\tpython3 post_links_crawler.py {query}")
        sys.exit(1)

    query = sys.argv[1]
    df = extract_total_links(query)
    df = transform(df)
    load(query, df)
