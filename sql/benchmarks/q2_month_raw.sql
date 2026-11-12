-- Benchmark Q2 on raw gzipped JSON: one month, January 2023. Nothing to prune:
-- the downloaded files are not split by month, so every line is read.
SELECT category, count(*) AS reviews, round(avg(rating), 4) AS avg_rating,
       round(avg(CAST(verified_purchase AS integer)), 4) AS verified_share
FROM rrl_raw.reviews
WHERE user_id IS NOT NULL AND "timestamp" >= 1672531200000 AND "timestamp" < 1675209600000
GROUP BY 1
ORDER BY 1
