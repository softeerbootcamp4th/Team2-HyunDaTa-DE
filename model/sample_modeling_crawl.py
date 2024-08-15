import argparse
import unicodedata
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col


""" Sample Modeling Script
This script is a sample modeling script that calculates the emerge score of a car based on the issue.
emerge score = View + Like * 20 + (Title_wc + Body_wc + Comment_wc) * 10
"""


JDBC_URL = "db_url"
DB_NAME = "dbname"
DB_USER = "dbusername"
DB_PASSWORD = "dbpassword"


def extract_contents_contains_car_name(bucket_name: str, base_path: str, car_name: str):
    return f"s3://{bucket_name}/{base_path}/*/*{car_name}*.csv"


def save_to_rds(df, rds_url, rds_table, user, password):
    df.coalesce(1).write.jdbc(
        url=rds_url,
        table=rds_table,
        mode="append",
        properties={
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
    )


def calculate_emerge_score(path_pattern: str, car_name: str, issue: str):
    spark = SparkSession.builder.appName(
        f"Calculate {car_name}'s Issue: {issue}").getOrCreate()

    print("path_pattern: ", path_pattern)
    tmp_df = spark.read.csv(
        path_pattern,
        header=True,
        quote='"',
        escape='"',
        multiLine=True,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
    )

    tmp_df = tmp_df.withColumn("View", col("View").cast("int")) \
        .withColumn("Like", col("Like").cast("int"))
    tmp_df.show(5)
    tmp_df.printSchema()
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
    wc_df.show(5)
    wc_df.printSchema()
    wc_df.createOrReplaceTempView("wc_df")
    final_query = """
    SELECT 
        TRIM(Date) AS upload_date, 
        SUM(View) AS total_view,
        SUM(Like) AS total_like,
        SUM(Title_wc + Body_wc + Comment_wc) AS total_wc,
        total_view + total_like * 200 + total_wc * 1000 AS graph_score
    FROM wc_df
    GROUP BY TRIM(Date)
    ORDER BY TRIM(Date)
    """

    final_df = spark.sql(final_query)
    final_df = final_df.withColumn("issue", lit(issue))
    final_df = final_df.withColumn("carname", lit(car_name))
    final_df.show(5)
    save_to_rds(
        final_df,
        JDBC_URL,
        DB_NAME,
        DB_USER,
        DB_PASSWORD
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--bucket_name', help="The name of the S3 bucket.")
    parser.add_argument(
        '--base_path', help="The base path of the S3 bucket.")
    parser.add_argument(
        '--car_name', help="The name of the car you want to analyze. [genesis, palisade, casper, ...]")
    parser.add_argument(
        '--issue', help="The issue you want to analyze.")
    args = parser.parse_args()

    path_pattern = extract_contents_contains_car_name(
        args.bucket_name, args.base_path, args.car_name)

    issue = unicodedata.normalize("NFC", args.issue)
    calculate_emerge_score(
        path_pattern,
        args.car_name,
        issue
    )
