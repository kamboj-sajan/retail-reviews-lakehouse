-- The same user reviewing the same product at more than one time. These are
-- NOT merged by the chosen key; this counts how many there are, and how many
-- repeat exactly the same text. Reads the raw gzipped JSON.
WITH pairs AS (
    SELECT
        category,
        count(DISTINCT "timestamp") AS times,
        count(DISTINCT md5(to_utf8(coalesce(text, '')))) AS texts
    FROM rrl_raw.reviews
    WHERE user_id IS NOT NULL
    GROUP BY category, user_id, parent_asin
)
SELECT
    category,
    count_if(times > 1) AS user_product_pairs_with_several_times,
    count_if(times > 1 AND texts = 1) AS of_which_identical_text
FROM pairs
GROUP BY category
ORDER BY category
