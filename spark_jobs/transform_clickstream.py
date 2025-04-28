from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
import argparse
from config.config_loader import AWS_CONFIG, SPARK_CONFIG
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("Clickstream Transformation") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_CONFIG['access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_CONFIG['secret']) \
        .config("spark.python.executable", SPARK_CONFIG['pyspark_python']) \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("page", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("location", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("search_query", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_id", StringType(), True),
        StructField("total_amount", DoubleType(), True)
    ])

def transform_clickstream_data(spark, input_path, output_path, start_date, end_date):
    raw_df = spark.read.json(
        input_path,
        schema=define_schema()
    ).filter(
        (col("timestamp") >= start_date) &
        (col("timestamp") <= end_date)
    )

    
    user_features = raw_df.groupBy("user_id").agg(
        count("*").alias("total_events"),
        count("search_query").alias("total_searches"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("total_cart_adds"),
        count(when(col("event_type") == "purchase", 1)).alias("total_purchases"),
        avg("total_amount").alias("avg_purchase_amount"),
        expr("count(distinct session_id)").alias("total_sessions"),
        expr("count(distinct page)").alias("unique_pages_visited")
    )

    
    device_distribution = raw_df.groupBy("user_id", "device_type").count()\
        .groupBy("user_id")\
        .pivot("device_type")\
        .agg(expr("first(count)"))\
        .fillna(0)

    
    final_features = user_features.join(device_distribution, "user_id")

    
    final_features.write.mode("overwrite").parquet(output_path)

def main():
    parser = argparse.ArgumentParser(description='Transform Clickstream Data')
    parser.add_argument('--input-path', required=True,help='Input path for raw clickstream data')
    parser.add_argument('--output-path', required=True,help='Output path for transformed features')
    parser.add_argument('--start-date', required=True,help='Start date for processing (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True,help='End date for processing (YYYY-MM-DD)')

    args = parser.parse_args()
    
    spark = create_spark_session()
    
    transform_clickstream_data(
        spark,
        args.input_path,
        args.output_path,
        args.start_date,
        args.end_date
    )

    spark.stop()

if __name__ == '__main__':
    main()