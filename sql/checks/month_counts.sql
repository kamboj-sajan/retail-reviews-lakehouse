-- Row counts for one month in every table the monthly load writes, plus the
-- cleaned table's total over all months. Run before and after loading a
-- month: the month's numbers must not change on a re-run, and the total
-- shows that no other month was touched.
--
-- ${month} is filled in by scripts/run_athena_query.py --param month=YYYY-MM
SELECT 'rrl_cleaned.reviews: rows in month' AS measure, count(*) AS value
FROM rrl_cleaned.reviews WHERE review_month = '${month}'
UNION ALL
SELECT 'rrl_cleaned.reviews: distinct review_id in month', count(DISTINCT review_id)
FROM rrl_cleaned.reviews WHERE review_month = '${month}'
UNION ALL
SELECT 'rrl_curated.reviews_enriched: rows in month', count(*)
FROM rrl_curated.reviews_enriched WHERE review_month = '${month}'
UNION ALL
SELECT 'rrl_curated.category_monthly: sum of review_count', coalesce(sum(review_count), 0)
FROM rrl_curated.category_monthly WHERE review_month = '${month}'
UNION ALL
SELECT 'rrl_curated.category_daily: sum of review_count', coalesce(sum(review_count), 0)
FROM rrl_curated.category_daily WHERE review_month = '${month}'
UNION ALL
SELECT 'rrl_curated.product_monthly: sum of review_count', coalesce(sum(review_count), 0)
FROM rrl_curated.product_monthly WHERE review_month = '${month}'
UNION ALL
SELECT 'rrl_cleaned.reviews: rows in all months', count(*)
FROM rrl_cleaned.reviews
