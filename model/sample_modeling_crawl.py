import argparse
import unicodedata
from pyspark.sql import SparkSession


""" Sample Modeling Script
This script is a sample modeling script that calculates the emerge score of a car based on the issue.
emerge score = View + Like * 20 + (Title_wc + Body_wc + Comment_wc) * 10
"""


def extract_contents_contains_car_name(bucket_name: str, base_path: str, car_name: str):
    return f"s3://{bucket_name}/{base_path}/*{car_name}*.csv"


def calculate_emerge_score(path_pattern: str, output_uri: str, car_name: str, issue: str):
    spark = SparkSession.builder.appName(
        f"Calculate {car_name}'s Issue: {issue}").getOrCreate()

    print(len(path_pattern))
    tmp_df = spark.read.csv(
        path_pattern,
        header=True,
        quote='"',
        escape='"',
        multiLine=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True
    )

    tmp_df.createOrReplaceTempView("data_df")
    issue = unicodedata.normalize('NFC', issue)

    sql_query = f"""
    SELECT Date,
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
    SELECT Date,
           SUM(View) AS Total_View,
           SUM(Like) AS Total_Like,
           SUM(Title_wc + Body_wc + Comment_wc) AS Total_wc,
           SUM(View + Like * 20 + (Title_wc + Body_wc + Comment_wc) * 10) AS Graph_score
    FROM wc_df
    GROUP BY Date
    ORDER BY Date
    """

    final_df = spark.sql(final_query)
    final_df.coalesce(1).write.option(
        "header", "true").mode("overwrite").csv(output_uri)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket_name', help="The name of the S3 bucket.")
    parser.add_argument(
        '--base_path', help="The base path of the S3 bucket.")
    parser.add_argument(
        '--output_uri', help="The URI where output is saved, like an S3 bucket location.")
    parser.add_argument(
        '--car_name', help="The name of the car you want to analyze. [genesis, palisade, casper, ...]")
    parser.add_argument(
        '--issue', help="The issue you want to analyze.")
    args = parser.parse_args()

    bucket_name, base_path, output_uri = args.bucket_name, args.base_path, args.output_uri
    path_pattern = extract_contents_contains_car_name(
        bucket_name, base_path, args.car_name)

    issue = args.issue.encode("utf-8").decode("utf-8")
    calculate_emerge_score(
        path_pattern,
        output_uri,
        args.car_name,
        issue
    )
