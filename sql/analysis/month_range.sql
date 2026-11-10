-- First and last month with reviews, and review volume in the most recent
-- months. Used to choose which months the incremental replay delivers.
SELECT
    review_month,
    sum(review_count) AS reviews,
    count(*) AS categories,
    min(min_month) AS first_month_overall
FROM (
    SELECT review_month, review_count, min(review_month) OVER () AS min_month
    FROM rrl_curated.category_monthly
)
GROUP BY review_month
ORDER BY review_month DESC
LIMIT 15
