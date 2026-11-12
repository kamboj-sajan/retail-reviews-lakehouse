-- Benchmark Q1 on cleaned Parquet: reads only review_date and rating.
SELECT category, year(review_date) AS year, count(*) AS reviews, round(avg(rating), 4) AS avg_rating
FROM rrl_cleaned.reviews
GROUP BY 1, 2
ORDER BY 1, 2
