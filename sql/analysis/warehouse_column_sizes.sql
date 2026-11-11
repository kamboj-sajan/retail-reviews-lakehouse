-- Longest value, in UTF-8 bytes, of every text column the warehouse loads.
-- Redshift VARCHAR(n) counts bytes, not characters, and COPY from Parquet
-- cannot truncate: one value longer than n fails the whole load. These
-- maxima set the column sizes in sql/warehouse/schema.sql.
SELECT 'reviews_enriched' AS source, 'user_id' AS column_name, max(length(to_utf8(user_id))) AS max_bytes FROM rrl_curated.reviews_enriched
UNION ALL SELECT 'reviews_enriched', 'asin', max(length(to_utf8(asin))) FROM rrl_curated.reviews_enriched
UNION ALL SELECT 'reviews_enriched', 'parent_asin', max(length(to_utf8(parent_asin))) FROM rrl_curated.reviews_enriched
UNION ALL SELECT 'products', 'parent_asin', max(length(to_utf8(parent_asin))) FROM rrl_cleaned.products
UNION ALL SELECT 'products', 'title', max(length(to_utf8(title))) FROM rrl_cleaned.products
UNION ALL SELECT 'products', 'store', max(length(to_utf8(store))) FROM rrl_cleaned.products
UNION ALL SELECT 'products', 'main_category', max(length(to_utf8(main_category))) FROM rrl_cleaned.products
UNION ALL SELECT 'products', 'category_path', max(length(to_utf8(array_join(categories, ' > ')))) FROM rrl_cleaned.products
UNION ALL SELECT 'products', 'max_price', CAST(max(price) AS bigint) FROM rrl_cleaned.products
