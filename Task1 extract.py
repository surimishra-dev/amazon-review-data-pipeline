#using Tidb
from pyspark.sql import SparkSession # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F # type: ignore
def get_spark_session():
    spark = (
        SparkSession.builder.appName("DataTransformationToBigQuery")
        .getOrCreate()
    )
    return spark

df = spark.read.table("Workspace.raw_data.aws_raw_data_sheet_1")
df.show(10)
#df.printSchema()
