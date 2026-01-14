from pyspark.sql.functions import col, lower, regexp_replace, to_date, year, month


def clean_data(df):
    """
    Perform data cleaning
    """
    df = df.dropDuplicates()
    df = df.dropna(subset=["review_id", "product_title", "star_rating"])

    df = df.withColumn("star_rating", col("star_rating").cast("int")) \
           .withColumn("review_date", to_date(col("review_date")))

    df = df.withColumn("product_category", lower(col("product_category"))) \
           .withColumn("marketplace", lower(col("marketplace")))

    return df


def transform_data(df):
    """
    Feature engineering and text normalization
    """
    df = df.withColumn(
        "review_body",
        regexp_replace(lower(col("review_body")), "[^a-zA-Z ]", "")
    )

    df = df.withColumn("review_year", year(col("review_date"))) \
           .withColumn("review_month", month(col("review_date")))

    return df
