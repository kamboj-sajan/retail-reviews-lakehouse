-- Dashboard: products whose average rating fell the most between two windows.
-- Averages are rating_sum / review_count over each whole window (not an
-- average of monthly averages). Only products with at least ${min_reviews}
-- reviews in BOTH windows qualify, so a change is not noise from a handful
-- of reviews. qualifying_products counts every product that qualified.
WITH windows AS (
    SELECT
        category,
        parent_asin,
        max(product_title) AS product_title,
        max(store) AS store,
        sum(CASE WHEN review_month BETWEEN '${before_from}' AND '${before_to}' THEN review_count ELSE 0 END) AS before_reviews,
        sum(CASE WHEN review_month BETWEEN '${before_from}' AND '${before_to}' THEN rating_sum ELSE 0 END) AS before_rating_sum,
        sum(CASE WHEN review_month BETWEEN '${recent_from}' AND '${recent_to}' THEN review_count ELSE 0 END) AS recent_reviews,
        sum(CASE WHEN review_month BETWEEN '${recent_from}' AND '${recent_to}' THEN rating_sum ELSE 0 END) AS recent_rating_sum
    FROM rrl_curated.product_monthly
    WHERE review_month BETWEEN '${before_from}' AND '${recent_to}'
    GROUP BY category, parent_asin
),
qualifying AS (
    SELECT
        *,
        CAST(before_rating_sum AS double) / before_reviews AS before_avg,
        CAST(recent_rating_sum AS double) / recent_reviews AS recent_avg
    FROM windows
    WHERE before_reviews >= ${min_reviews} AND recent_reviews >= ${min_reviews}
)
SELECT
    category, parent_asin, product_title, store,
    before_reviews, round(before_avg, 3) AS before_avg,
    recent_reviews, round(recent_avg, 3) AS recent_avg,
    round(recent_avg - before_avg, 3) AS change,
    count(*) OVER () AS qualifying_products
FROM qualifying
ORDER BY recent_avg - before_avg
LIMIT 20
