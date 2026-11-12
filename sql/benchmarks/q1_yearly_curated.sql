-- Benchmark Q1 on the curated monthly aggregate: about 900 rows.
SELECT category, CAST(substr(review_month, 1, 4) AS integer) AS year, sum(review_count) AS reviews,
       round(CAST(sum(rating_sum) AS double) / sum(review_count), 4) AS avg_rating
FROM rrl_curated.category_monthly
GROUP BY 1, 2
ORDER BY 1, 2
