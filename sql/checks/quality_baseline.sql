-- Rule violations in the cleaned reviews loaded so far, per category.
-- The baseline for the phase 5 quality checks: what the real data contains
-- before any quarantine rules exist.
SELECT
    category,
    count(*) AS reviews,
    count_if(review_id IS NULL OR user_id IS NULL OR parent_asin IS NULL OR review_ts IS NULL) AS missing_key_part,
    count_if(rating IS NULL) AS rating_not_whole_number,
    count_if(rating NOT BETWEEN 1 AND 5) AS rating_outside_1_to_5,
    count_if(review_month IS NULL OR review_month = '__HIVE_DEFAULT_PARTITION__') AS no_month,
    count(*) - count(DISTINCT review_id) AS duplicate_review_ids,
    min(review_ts) AS earliest_review,
    max(review_ts) AS latest_review
FROM rrl_cleaned.reviews
GROUP BY category
ORDER BY category
