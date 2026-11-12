-- Benchmark Q2 on the curated monthly aggregate: three rows read.
SELECT category, review_count AS reviews, round(CAST(rating_sum AS double) / review_count, 4) AS avg_rating,
       round(CAST(verified_count AS double) / review_count, 4) AS verified_share
FROM rrl_curated.category_monthly
WHERE review_month = '2023-01'
ORDER BY 1
