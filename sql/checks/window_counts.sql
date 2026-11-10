-- Per-month row counts over a range of months, in every table the monthly
-- load writes. Run once on the backfill and once after the replay: the two
-- results must be identical, which shows that loading month by month gives
-- exactly what the full load gave.
--
-- ${from} and ${to} are filled in by scripts/run_athena_query.py --param.
WITH cleaned AS (
    SELECT review_month, count(*) AS cleaned_reviews, count(DISTINCT review_id) AS distinct_review_ids
    FROM rrl_cleaned.reviews WHERE review_month BETWEEN '${from}' AND '${to}' GROUP BY review_month
), enriched AS (
    SELECT review_month, count(*) AS enriched_reviews
    FROM rrl_curated.reviews_enriched WHERE review_month BETWEEN '${from}' AND '${to}' GROUP BY review_month
), monthly AS (
    SELECT review_month, sum(review_count) AS category_monthly_reviews, sum(rating_sum) AS rating_sum
    FROM rrl_curated.category_monthly WHERE review_month BETWEEN '${from}' AND '${to}' GROUP BY review_month
), daily AS (
    SELECT review_month, sum(review_count) AS category_daily_reviews
    FROM rrl_curated.category_daily WHERE review_month BETWEEN '${from}' AND '${to}' GROUP BY review_month
), product AS (
    SELECT review_month, sum(review_count) AS product_monthly_reviews, count(*) AS product_month_rows
    FROM rrl_curated.product_monthly WHERE review_month BETWEEN '${from}' AND '${to}' GROUP BY review_month
)
SELECT c.review_month, c.cleaned_reviews, c.distinct_review_ids, e.enriched_reviews,
       m.category_monthly_reviews, d.category_daily_reviews, p.product_monthly_reviews,
       p.product_month_rows, m.rating_sum
FROM cleaned c
FULL OUTER JOIN enriched e ON e.review_month = c.review_month
FULL OUTER JOIN monthly m ON m.review_month = c.review_month
FULL OUTER JOIN daily d ON d.review_month = c.review_month
FULL OUTER JOIN product p ON p.review_month = c.review_month
ORDER BY c.review_month
