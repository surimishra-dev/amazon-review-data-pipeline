CREATE TABLE amazon_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    product_title VARCHAR(255),
    product_category VARCHAR(100),
    star_rating INT,
    helpful_votes INT,
    total_votes INT,
    customer_id VARCHAR(50),
    review_body TEXT,
    review_date DATE,
    review_year INT,
    review_month INT
);
