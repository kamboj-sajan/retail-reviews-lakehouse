-- The cleaned reviews whose rating is outside 1 to 5.
SELECT category, review_month, review_id, parent_asin, rating, helpful_vote, verified_purchase, review_ts
FROM rrl_cleaned.reviews
WHERE rating NOT BETWEEN 1 AND 5
ORDER BY review_ts
