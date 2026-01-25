from pyspark.sql.functions import col, lower, trim, regexp_replace, date_format

# Standardize review_date to YYYY-MM-DD
transformed_df = df.withColumn("review_date", date_format(col("review_date"), "yyyy-MM-dd"))

# Normalize text fields: lowercase, trim, remove unwanted characters (non-alphanumeric except space)
for text_col in ["review_body", "review_headline"]:
    transformed_df = transformed_df.withColumn(
        text_col,
        lower(trim(regexp_replace(col(text_col), "[^a-zA-Z0-9 ]", "")))
    )

# Create additional feature: review_month
transformed_df = transformed_df.withColumn("review_month", date_format(col("review_date"), "yyyy-MM"))

# Show transformed data
transformed_df.show(10)
# transformed_df.printSchema()
transformed_df.write.mode("overwrite").saveAsTable("Workspace.raw_data.transformed_reviews")
