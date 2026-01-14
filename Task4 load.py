from sqlalchemy import create_engine
import pandas as pd


def load_to_rds(df, db_config):
    """
    Load Spark DataFrame to AWS RDS using batch inserts
    """
    pandas_df = df.toPandas()

    engine = create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

    pandas_df.to_sql(
        name="amazon_reviews",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000
    )
