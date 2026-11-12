-- Benchmark Q3 on raw gzipped JSON: reviews whose text mentions "battery".
SELECT category, count(*) AS reviews_mentioning_battery
FROM rrl_raw.reviews
WHERE user_id IS NOT NULL AND lower(text) LIKE '%battery%'
GROUP BY 1
ORDER BY 1
