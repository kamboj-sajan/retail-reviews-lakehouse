-- Benchmark Q2 on cleaned Parquet: only the review_month=2023-01 folders are read.
SELECT category, count(*) AS reviews, round(avg(rating), 4) AS avg_rating,
       round(avg(CAST(verified_purchase AS integer)), 4) AS verified_share
FROM rrl_cleaned.reviews
WHERE review_month = '2023-01'
GROUP BY 1
ORDER BY 1
