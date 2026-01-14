from pyspark.sql import SparkSession


def create_spark_session():
    return SparkSession.builder \
        .appName("AmazonReviewPipeline") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()


def extract_data(spark, s3_path):
    """
    Extract Parquet data from AWS S3
    """
    return spark.read.parquet(s3_path)
