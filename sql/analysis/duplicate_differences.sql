-- For reviews that appear more than once under the chosen key
-- (user_id, parent_asin, timestamp): how many copies, and which fields
-- differ between the copies. Reads the raw gzipped JSON.
WITH repeated AS (
    SELECT
        category,
        count(*) AS copies,
        count(DISTINCT asin) AS asins,
        count(DISTINCT rating) AS ratings,
        count(DISTINCT helpful_vote) AS votes,
        max(helpful_vote) - min(helpful_vote) AS vote_spread,
        count(DISTINCT verified_purchase) AS verified_flags,
        count(DISTINCT md5(to_utf8(coalesce(title, '')))) AS titles,
        count(DISTINCT md5(to_utf8(coalesce(text, '')))) AS texts
    FROM rrl_raw.reviews
    WHERE user_id IS NOT NULL
    GROUP BY category, user_id, parent_asin, "timestamp"
    HAVING count(*) > 1
)
SELECT
    category,
    count(*) AS repeated_reviews,
    sum(copies - 1) AS extra_copies,
    max(copies) AS most_copies,
    count_if(asins > 1) AS differ_in_asin,
    count_if(ratings > 1) AS differ_in_rating,
    count_if(votes > 1) AS differ_in_helpful_vote,
    max(vote_spread) AS largest_vote_difference,
    count_if(verified_flags > 1) AS differ_in_verified,
    count_if(titles > 1) AS differ_in_title,
    count_if(texts > 1) AS differ_in_text
FROM repeated
GROUP BY category
ORDER BY category
