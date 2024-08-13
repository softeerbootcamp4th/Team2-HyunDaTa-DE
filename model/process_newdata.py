import os
import argparse
import unicodedata
from pyspark.sql import SparkSession


""" Sample Modeling Script
This script is a sample modeling script that calculates the emerge score of a car based on the issue.
emerge score = View + Like * 20 + (Title_wc + Body_wc + Comment_wc) * 10
"""


def extract_new_uploaded_contents(
    bucket_name: str,
    base_path: str,
    date: str,
    time: str,
    car_name: str
) -> str:
    return f"s3://{bucket_name}/{base_path}/*/{date}/{date}_{time}*{car_name}.csv"


def save_to_rds(df, rds_url, rds_table, rds_properties):
    df.write.jdbc(url=rds_url, table=rds_table,
                  mode="append", properties=rds_properties)


def calculate_emerge_score(path_pattern: str, output_uri: str, car_name: str, issue: str):
    # SparkSession 생성
    spark = SparkSession.builder.appName(
        f"Calculate {car_name}'s Issue: {issue}").getOrCreate()

    # 파일들을 불러오기
    tmp_df = spark.read.csv(
        path_pattern,
        header=True,
        quote='"',
        escape='"',
        multiLine=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True
    )

    # DataFrame을 테이블로 등록
    tmp_df.createOrReplaceTempView("data_df")
    sql_query = f"""
    SELECT 
        Date,
        CASE WHEN View IS NULL THEN 0 ELSE View END AS View,
        CASE WHEN Like IS NULL THEN 0 ELSE Like END AS Like,
        CASE WHEN Title IS NULL THEN 0 ELSE SIZE(SPLIT(Title, '{issue}')) - 1 END AS Title_wc,
        CASE WHEN Body IS NULL THEN 0 ELSE SIZE(SPLIT(Body, '{issue}')) - 1 END AS Body_wc,
        CASE WHEN Comment IS NULL THEN 0 ELSE SIZE(SPLIT(Comment, '{issue}')) - 1 END AS Comment_wc
    FROM data_df
    """

    wc_df = spark.sql(sql_query)
    wc_df.createOrReplaceTempView("wc_df")
    final_query = """
    SELECT 
        Date,
        SUM(View) AS Total_View,
        SUM(Like) AS Total_Like,
        SUM(Title_wc + Body_wc + Comment_wc) AS Total_wc,
        SUM(View + Like * 20 + (Title_wc + Body_wc + Comment_wc) * 10) AS Graph_score
    FROM wc_df
    GROUP BY Date
    ORDER BY Date
    """

    final_df = spark.sql(final_query)

    # 데이터를 하나의 파일로 저장
    final_df.coalesce(1).write.option(
        "header", "true").mode("overwrite").csv(output_uri)

    # SparkSession 종료
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket_name', help="The name of the S3 bucket. (e.g. 'my_bucket')")
    parser.add_argument(
        '--base_path', help="The base path of the S3 bucket. (e.g. 'my_folder')")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location. (e.g. 's3://bucket_name/output')")
    parser.add_argument(
        '--date', help="The date you want to analyze. (e.g. '20210101')")
    parser.add_argument(
        '--time', help="The time you want to analyze. (e.g. '0100')")
    parser.add_argument(
        '--car_name', help="The name of the car you want to analyze. [genesis, palisade, casper, ...]")
    parser.add_argument(
        '--issue', help="The issue you want to analyze.")
    args = parser.parse_args()

    # S3 경로 패턴 설정
    path_pattern = extract_new_uploaded_contents(
        args.bucket_name, args.base_path, args.date, args.time, args.car_name)

    issue = unicodedata.normalize("NFC", args.issue)
    calculate_emerge_score(
        path_pattern,
        args.output_uri,
        args.car_name,
        issue
    )
