-- Top 10 products with most reviews
SELECT product_title, COUNT(*) AS review_count
FROM transformed_reviews
GROUP BY product_title
ORDER BY review_count DESC
LIMIT 10;

-- Average rating per month per product
SELECT product_title, review_year, review_month, AVG(star_rating) avg_rating
FROM transformed_reviews
GROUP BY product_title, review_year, review_month;

-- Total votes per product category
SELECT product_category, SUM(total_votes) total_votes
FROM transformed_reviews
GROUP BY product_category;

-- Products with "awful"
SELECT product_title, COUNT(*) cnt
FROM transformed_reviews
WHERE review_body LIKE '%awful%'
GROUP BY product_title
ORDER BY cnt DESC;

-- Products with "awesome"
SELECT product_title, COUNT(*) cnt
FROM transformed_reviews
WHERE review_body LIKE '%awesome%'
GROUP BY product_title
ORDER BY cnt DESC;

-- Controversial reviews
SELECT review_id, total_votes, helpful_votes
FROM transformed_reviews
WHERE total_votes > 50
AND (helpful_votes / total_votes) < 0.3;

-- Most reviewed product per year
SELECT review_year, product_title, COUNT(*) cnt
FROM transformed_reviews
GROUP BY review_year, product_title
ORDER BY cnt DESC;

-- Most active reviewers
SELECT customer_id, COUNT(*) review_count
FROM transformed_reviews
GROUP BY customer_id
ORDER BY review_count DESC;
