-- Benchmark Q1 on raw gzipped JSON: reviews and average rating per category and year.
-- Reads every line; includes the duplicate copies and the two rating-0 reviews.
SELECT category, year(from_unixtime("timestamp" / 1000)) AS year, count(*) AS reviews, round(avg(rating), 4) AS avg_rating
FROM rrl_raw.reviews
WHERE user_id IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
