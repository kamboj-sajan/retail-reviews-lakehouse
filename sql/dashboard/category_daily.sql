-- Dashboard: reviews per category and day, for the sale-period panels.
SELECT category, CAST(review_date AS varchar) AS review_date, review_count, rating_sum
FROM rrl_curated.category_daily
WHERE review_month BETWEEN '${from}' AND '${to}'
ORDER BY category, review_date
