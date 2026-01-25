from pyspark.sql.functions import col, lower, trim, to_date

# Remove duplicates
cleaned_df = df.dropDuplicates()

# Handle missing/null values
# Drop rows with nulls in critical columns
critical_cols = ["review_date", "star_rating", "product_category", "marketplace"]
cleaned_df = cleaned_df.dropna(subset=critical_cols)
# Fill nulls in text columns with 'Unknown'
text_cols = ["product_category", "marketplace", "product_title"]
cleaned_df = cleaned_df.fillna("Unknown", subset=text_cols)

# Correct data types
cleaned_df = cleaned_df.withColumn("review_date", to_date(col("review_date")))
cleaned_df = cleaned_df.withColumn("star_rating", col("star_rating").cast("int"))

# Standardize categorical fields
cleaned_df = cleaned_df.withColumn("product_category", lower(trim(col("product_category"))))
cleaned_df = cleaned_df.withColumn("marketplace", lower(trim(col("marketplace"))))

# Show cleaned data
cleaned_df.show(10)
# cleaned_df.printSchema()
