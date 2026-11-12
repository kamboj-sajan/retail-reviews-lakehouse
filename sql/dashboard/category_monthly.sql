-- Dashboard: reviews and rating per category and month (rating trend, volume, headline numbers).
SELECT category, review_month, review_count, rating_sum, verified_count
FROM rrl_curated.category_monthly
ORDER BY category, review_month
