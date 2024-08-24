import argparse
import numpy as np
from dtaidistance import dtw
from datetime import timedelta, datetime

import pymysql
from pyspark import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Row, Window
from pyspark.sql.types import StringType, ArrayType, DoubleType, StructType, StructField, DateType, FloatType


JDBC_URL = ""
HOST = ""
DB_USER = ""
DB_PASSWORD = ""
DB_NAME = ""
POST_DB_NAME = ""
RECOMMEND_DB_NAME = ""


def save_recommend_table_to_rds(df):
    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", RECOMMEND_DB_NAME) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()


def save_trigger_table_to_rds(df_list):
    connection = pymysql.connect(
        host=HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )
    try:
        with connection.cursor() as cursor:
            sql = """
            INSERT INTO issue_trigger (car_name, issue, upload_date, is_trigger, url)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                issue = VALUES(issue),
                upload_date = VALUES(upload_date),
                is_trigger = VALUES(is_trigger),
                url = VALUES(url)
            """
            cursor.executemany(sql, df_list)

        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()

    finally:
        connection.close()


def filter_by_date(row, cur_time: str):
    end_date = datetime.strptime(cur_time, '%Y-%m-%d %H:%M').date()
    start_date = end_date - timedelta(days=90)
    return start_date <= row["upload_date"] <= end_date


def calculate_graph_score(partition, alpha=0.05, beta=0.02):
    view_moving_avg, like_moving_avg = None, None
    result = []
    for row in partition:
        c_view_value, c_like_value = row["normalized_avg_log_view"], row["normalized_sum_log_like"]
        if view_moving_avg is None:
            view_moving_avg = c_view_value
        else:
            view_moving_avg = (1-alpha) * view_moving_avg + \
                alpha * c_view_value

        if like_moving_avg is None:
            like_moving_avg = c_like_value
        else:
            like_moving_avg = (1-beta) * like_moving_avg + beta * c_like_value
        result.append(
            Row(
                car_name=row["car_name"],
                indiv_issue=row["indiv_issue"],
                upload_date=row["upload_date"],
                normalized_avg_log_view=c_view_value,
                normalized_sum_log_like=c_like_value,
                moving_avg_normalized_mean_log_view=view_moving_avg,
                moving_avg_normalized_sum_log_like=like_moving_avg
            )
        )
    return iter(result)


def find_trigger_points(index, iterator, window_size=7, std_threshold=2.1):
    results = []
    data_list = list(iterator)

    # 컬럼별로 필터링 작업
    for i in range(window_size-1, len(data_list)):
        current_row = data_list[i]
        if (data_list[i-window_size+1][0] != current_row[0]) | (data_list[i-window_size+1][1] != current_row[1]):
            continue

        # 필터링 조건 예시 (필요에 따라 수정)
        if not isinstance(current_row[3], (int, float)):
            continue  # 'Value' 컬럼이 숫자가 아니면 무시

        # 이동 평균 및 표준 편차 계산
        window_data = [
            float(x[3]) for x in data_list[i-window_size+1:i+1]
            if isinstance(x[3], (int, float))
        ]

        moving_avg = np.mean(window_data)
        moving_std = np.std(window_data)
        current_value = float(current_row[3])

        # 트리거 조건을 체크
        if len(window_data) == window_size and current_value - moving_avg > std_threshold * moving_std:
            results.append(
                Row(
                    car_name=current_row[0],
                    issue=current_row[1],
                    trigger_date=current_row[2]
                )
            )

    return iter(results)


def compute_dtw(a_list, b_list, trigger_date):
    a_scores = [x['norm_graph_score']
                for x in a_list if x['norm_graph_score'] is not None]
    filtered_b_list = [
        x for x in b_list
        if x['norm_graph_score'] is not None and
        x['upload_date'] <= trigger_date
    ]

    # Extract scores from the filtered b_list
    b_scores = [x['norm_graph_score'] for x in filtered_b_list]

    if not a_scores or not b_scores:
        return float('inf')
    return float(dtw.distance(a_scores, b_scores))


def adjust_dates(recommend_json, current_time):
    adjusted_recommendations = []
    for recommend in recommend_json:
        recommend_dict = recommend.asDict()
        trigger_date = datetime.strptime(recommend_dict['recommend_start_date'], '%Y-%m-%d').date() \
            if isinstance(recommend_dict['recommend_start_date'], str) \
            else recommend_dict['recommend_start_date']

        current_time = datetime.strptime(current_time, '%Y-%m-%d %H:%M').date() \
            if isinstance(current_time, str) \
            else current_time

        time_gap = (current_time - trigger_date).days
        adjusted_graph_scores = []
        for item in recommend_dict['recommend_graph_score_list']:
            item_dict = item.asDict()
            current_date = datetime.strptime(item_dict['upload_date'], '%Y-%m-%d').date() \
                if isinstance(item_dict['upload_date'], str) \
                else item_dict['upload_date']

            new_upload_date = current_date + timedelta(days=time_gap)
            item_dict['upload_date'] = new_upload_date.strftime('%Y-%m-%d')
            adjusted_graph_scores.append(item_dict)
        recommend_dict['recommend_graph_score_list'] = adjusted_graph_scores
        adjusted_recommendations.append(recommend_dict)
    return adjusted_recommendations


def interpolation(target, x):
    return target * (1.2**(-x)*3+1)


graph_score_schema = ArrayType(StructType([
    StructField("upload_date", StringType(), True),
    StructField("norm_graph_score", DoubleType(), True)
]))

recommend_schema = ArrayType(StructType([
    StructField("recommend_car_name", StringType(), True),
    StructField("recommend_issue", StringType(), True),
    StructField("recommend_start_date", DateType(), True),
    StructField("recommend_graph_score_list",
                ArrayType(StructType([
                    StructField("upload_date", StringType(), True),
                    StructField("norm_graph_score", DoubleType(), True)
                ])), True)
]))


def preprocess_rds_data(emr_run_date):
    spark = SparkSession.builder \
        .appName("RDS to Spark") \
        .config("spark.executor.memory", "20g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    df = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", POST_DB_NAME) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .load()

    # extract issue_list
    time_only = F.date_format(F.col('upload_time'), 'HH:mm:ss')
    df = df.drop(
        F.col('title'), F.col('body'), F.col('comments')
    ).filter(
        F.get_json_object(df.issue, '$[0]').isNotNull()
    ).withColumn(
        'merged_time',
        F.to_timestamp(F.concat(F.col('upload_date'), F.lit(' '), time_only))
    )

    # interpolation latest 48 hours data
    cur_date = datetime.strptime(emr_run_date, "%Y-%m-%d %H:%M")
    two_days_ago = cur_date - timedelta(hours=48)
    df_recent = df.filter(F.col('upload_date') >= two_days_ago)

    time_difference_in_seconds = F.unix_timestamp(
        F.lit(cur_date)) - F.unix_timestamp(F.col("merged_time"))
    time_difference_in_hours = time_difference_in_seconds / 3600
    df_recent = df_recent.withColumn('time_diff', time_difference_in_hours)

    interpolation_udf = F.udf(interpolation, FloatType())
    df_recent = df_recent.withColumn(
        'num_views',
        interpolation_udf(
            F.col('num_views'),
            F.col('time_diff')
        )
    ).withColumn(
        'num_likes',
        interpolation_udf(
            F.col('num_likes'),
            F.col('time_diff')
        )
    ).drop(
        'merged_time',
        'time_diff'
    )

    # join with interpolated recent data
    df = df.alias('a').join(
        df_recent.alias('b'),
        on='url',
        how='left'
    ).select(
        *[
            F.col('a.' + col_name)
            if col_name not in ['num_views', 'num_likes']
            else F.coalesce(F.col('b.' + col_name), F.col('a.' + col_name)).alias(col_name)
            for col_name in df.columns
        ]
    ).withColumn(
        'issue_list',
        F.from_json(
            F.col("issue"), ArrayType(StringType())
        )
    )

    defect_df = df.withColumn('indiv_issue', F.explode('issue_list'))
    defect_df = defect_df.drop(F.col("issue"), F.col("issue_list"))
    defect_df = defect_df.withColumn("log_views", F.log1p("num_views")) \
        .withColumn("log_likes", F.log1p("num_likes"))

    defect_group_df = defect_df.groupBy('community').agg(
        F.min('num_views').alias('min_view'),
        F.max('num_views').alias('max_view'),
        F.min('num_likes').alias('min_like'),
        F.max('num_likes').alias('max_like'),
        F.min('log_views').alias('min_log_view'),
        F.max('log_views').alias('max_log_view'),
        F.min('log_likes').alias('min_log_like'),
        F.max('log_likes').alias('max_log_like'),
    )
    defect_df = defect_df.join(F.broadcast(defect_group_df), on=['community'])
    defect_df = defect_df.withColumn(
        'normalized_view',
        (F.col('num_views') - F.col('min_view')) /
        (F.col('max_view') - F.col('min_view'))
    ).withColumn(
        'normalized_like',
        (F.col('num_likes') - F.col('min_like')) /
        (F.col('max_like') - F.col('min_like'))
    ).withColumn(
        'normalized_log_view',
        (F.col('log_views') - F.col('min_log_view')) /
        (F.col('max_log_view') - F.col('min_log_view'))
    ).withColumn(
        'normalized_log_like',
        (F.col('log_likes') - F.col('min_log_like')) /
        (F.col('max_log_like') - F.col('min_log_like'))
    ).drop(
        F.col("min_view"), F.col('max_view'), F.col(
            'min_like'), F.col('max_like'),
        F.col('min_log_view'), F.col('max_log_view'), F.col(
            'min_log_like'), F.col('max_log_like')
    )
    defect_grouped_df = defect_df.groupBy("indiv_issue", "car_name", 'upload_date').agg(
        F.avg("normalized_log_view").alias("normalized_avg_log_view"),
        F.sum("normalized_log_like").alias("normalized_sum_log_like")
    )
    defect_grouped_df = defect_grouped_df.orderBy(
        "car_name", "indiv_issue", "upload_date"
    )

    # change to rdd for calculating graph score (chaining)
    defect_rdd = defect_grouped_df.repartition(1).rdd
    sorted_rdd = defect_rdd.sortBy(
        lambda x: (
            x["car_name"],
            x["indiv_issue"],
            x["upload_date"]
        )
    )
    result_rdd = sorted_rdd.mapPartitions(
        lambda partition: calculate_graph_score(partition)
    )

    result_rdd = result_rdd.map(lambda row: Row(
        car_name=row[0],
        indiv_issue=row[1],
        upload_date=row[2],
        norm_graph_score=row[5] * row[6]  # view * like
    ))
    result_rdd.persist(StorageLevel.MEMORY_AND_DISK)

    # find recent 90 days data
    result_3month_rdd = result_rdd.filter(
        lambda row: filter_by_date(row, emr_run_date))

    trigger_3month_rdd = result_3month_rdd.mapPartitionsWithIndex(
        find_trigger_points)

    trigger_3month_rdd = trigger_3month_rdd.map(lambda row: Row(
        car_name=row.car_name,
        issue=row.issue,
        upload_date=row.trigger_date,
        is_trigger=True
    ))
    trigger_3month_df = spark.createDataFrame(trigger_3month_rdd)

    # match trigger points with urls
    trigger_url_df = defect_df.alias('a').join(
        trigger_3month_df.alias('b'),
        (F.col('a.car_name') == F.col('b.car_name')) &
        (F.col('a.indiv_issue') == F.col('b.issue')) &
        (F.col('a.upload_date') == F.col('b.upload_date')),
        'inner'
    ).select(
        'b.car_name', 'b.issue', 'b.upload_date', 'b.is_trigger', 'a.url'
    )

    trigger_url_df = trigger_url_df.groupBy(
        "car_name", "issue", "upload_date", "is_trigger"
    ).agg(
        F.collect_list(F.col('url')).alias('url')
    ).withColumn(
        'url', F.to_json(F.col('url'))
    )
    save_trigger_table_to_rds(trigger_url_df)

    # find all trigger points
    all_trigger_df_rdd = result_rdd.mapPartitionsWithIndex(find_trigger_points)
    all_trigger_df = spark.createDataFrame(all_trigger_df_rdd)

    result_df = spark.createDataFrame(result_rdd)
    all_data = all_trigger_df.alias("t").join(
        result_df.alias("d"),
        (F.col("t.car_name") == F.col("d.car_name")) &
        (F.col("t.issue") == F.col("d.indiv_issue")) &
        (F.col("d.upload_date").between(
            F.date_sub(F.col("t.trigger_date"), 90),
            F.date_add(F.col("t.trigger_date"), 90)
        )),
        "inner"
    ).select(
        F.col("t.car_name").alias('t_car_name'),
        F.col("t.issue").alias('t_issue'),
        F.col("t.trigger_date").alias("trigger_date"),
        F.col("d.upload_date").alias("data_upload_date"),
        F.col("d.norm_graph_score")
    ).orderBy(
        "t.car_name", "t.issue", "t.trigger_date", "d.upload_date"
    )

    all_data = all_data.groupBy("t_car_name", "t_issue", "trigger_date").agg(
        F.collect_list(
            F.struct(
                F.col("data_upload_date").alias("upload_date"),
                F.col("norm_graph_score")
            )
        ).alias("data_list")
    )
    all_data.persist(StorageLevel.MEMORY_AND_DISK)

    # get 3month data
    three_month_df = spark.createDataFrame(result_3month_rdd)
    three_month_data_df = three_month_df.orderBy('upload_date').groupBy("car_name", "indiv_issue").agg(
        F.collect_list(F.struct("upload_date", "norm_graph_score")).alias(
            "3month_data")
    )

    all_data = all_data.repartition(50)
    joined_df = all_data.alias("all").join(
        F.broadcast(three_month_data_df).alias("three"),
        (F.col("three.car_name") != F.col("all.t_car_name")) &
        (F.col("three.indiv_issue") != F.col("all.t_issue")),
        "inner"
    ).select(
        "car_name", "indiv_issue", "3month_data",
        "t_car_name", "t_issue", "trigger_date", "data_list"
    )

    compute_dtw_udf = F.udf(compute_dtw, DoubleType())
    distance_df = joined_df.withColumn(
        "dtw_distance",
        compute_dtw_udf(F.col("3month_data"), F.col(
            "data_list"), F.col("trigger_date"))
    )
    distance_df = distance_df.repartition(100)

    # define window spec for find min dtw distance (ranking 3)
    window_spec = Window.partitionBy(
        "car_name", "indiv_issue"
    ).orderBy(F.col("dtw_distance"))

    top3_recommendations = distance_df.withColumn(
        "rank", F.row_number().over(window_spec)
    ).filter(F.col("rank") <= 3)
    top3_recommendations.persist(StorageLevel.MEMORY_AND_DISK)

    # save top3_recommendations to rds
    recommendations_df = top3_recommendations.groupBy("car_name", "indiv_issue", "3month_data") \
        .agg(
        F.collect_list(
            F.struct(
                F.col("t_car_name").alias("recommend_car_name"),
                F.col("t_issue").alias("recommend_issue"),
                F.col("trigger_date").alias("recommend_start_date"),
                F.col("data_list").alias("recommend_graph_score_list"),
            )
        ).alias("recommend_json")
    ).withColumnRenamed(
        "car_name", "monitor_car_name"
    ).withColumnRenamed(
        "indiv_issue", "monitor_issue"
    ).withColumnRenamed(
        "3month_data", "monitor_graph_score_list"
    )

    adjust_dates_udf = F.udf(adjust_dates, recommend_schema)
    recommendations_df = recommendations_df.withColumn(
        "recommend_json",
        adjust_dates_udf(
            F.col("recommend_json"),
            F.lit(emr_run_date)
        )
    ).withColumn(
        "recommend_json",
        F.to_json(F.col("recommend_json"))
    ).withColumn(
        "monitor_graph_score_list",
        F.to_json(F.col("monitor_graph_score_list"))
    )
    save_recommend_table_to_rds(recommendations_df)
    spark.stop()


if __name__ == "__main__":
    argparse = argparse.ArgumentParser()
    argparse.add_argument("--emr_run_date", type=str)

    args = argparse.parse_args()
    preprocess_rds_data(args.emr_run_date)
