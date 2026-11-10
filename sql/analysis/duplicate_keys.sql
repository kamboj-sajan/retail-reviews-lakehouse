-- How many review rows repeat an earlier row, under each candidate
-- de-duplication key, per category. Reads the raw gzipped JSON.
--
-- "Beyond the first" = rows minus distinct key values: the number of rows a
-- de-duplication on that key would remove.
--
-- Keys are hashed with MD5 before counting distinct values, so Athena keeps
-- 16 bytes per key in memory instead of the full review text.
-- Malformed JSON lines come back as all-NULL rows (ignore.malformed.json) and
-- are counted separately.
SELECT
    category,
    count_if(user_id IS NULL) AS unreadable_lines,
    count_if(user_id IS NOT NULL) AS review_rows,
    count_if(user_id IS NOT NULL) - count(DISTINCT IF(user_id IS NOT NULL, md5(to_utf8(concat_ws('|',
        user_id, asin, parent_asin, CAST("timestamp" AS varchar), CAST(rating AS varchar),
        CAST(helpful_vote AS varchar), CAST(verified_purchase AS varchar),
        coalesce(title, ''), coalesce(text, '')))))) AS beyond_first_every_field,
    count_if(user_id IS NOT NULL) - count(DISTINCT IF(user_id IS NOT NULL, md5(to_utf8(concat_ws('|',
        user_id, asin, CAST("timestamp" AS varchar)))))) AS beyond_first_user_asin_time,
    count_if(user_id IS NOT NULL) - count(DISTINCT IF(user_id IS NOT NULL, md5(to_utf8(concat_ws('|',
        user_id, parent_asin, CAST("timestamp" AS varchar)))))) AS beyond_first_user_product_time,
    count_if(user_id IS NOT NULL) - count(DISTINCT IF(user_id IS NOT NULL, md5(to_utf8(concat_ws('|',
        user_id, CAST("timestamp" AS varchar)))))) AS beyond_first_user_time,
    count_if(user_id IS NOT NULL) - count(DISTINCT IF(user_id IS NOT NULL, md5(to_utf8(concat_ws('|',
        user_id, parent_asin))))) AS beyond_first_user_product
FROM rrl_raw.reviews
GROUP BY category
ORDER BY category
