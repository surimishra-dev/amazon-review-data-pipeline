from extract import create_spark_session, extract_data
from transform import clean_data, transform_data
from load import load_to_rds
from config.db_config import DB_CONFIG
from utils import log


def main():
    spark = create_spark_session()

    log("Starting extraction")
    df = extract_data(
        spark,
        "s3a://amazon-review-bucket/reviews/"
    )

    log("Cleaning data")
    df_clean = clean_data(df)

    log("Transforming data")
    df_transformed = transform_data(df_clean)

    log("Loading data to RDS")
    load_to_rds(df_transformed, DB_CONFIG)

    log("Pipeline execution completed successfully")


if __name__ == "__main__":
    main()
