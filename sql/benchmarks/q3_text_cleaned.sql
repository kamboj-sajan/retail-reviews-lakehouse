-- Benchmark Q3 on cleaned Parquet: needs the text column, the biggest one,
-- so this is where Parquet's column skipping helps least.
SELECT category, count(*) AS reviews_mentioning_battery
FROM rrl_cleaned.reviews
WHERE lower(text) LIKE '%battery%'
GROUP BY 1
ORDER BY 1
