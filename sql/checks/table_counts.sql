-- Row count of every cleaned and curated table, per category where the table
-- has one. Proves each catalog table is queryable through Athena, and gives
-- the counts to reconcile against the job metrics.
--
-- Counting Parquet is cheap: Athena reads the row counts stored in each
-- file's footer, not the data.
SELECT 'rrl_cleaned.reviews' AS table_name, category, count(*) AS row_count FROM rrl_cleaned.reviews GROUP BY category
UNION ALL
SELECT 'rrl_cleaned.products', category, count(*) FROM rrl_cleaned.products GROUP BY category
UNION ALL
SELECT 'rrl_curated.reviews_enriched', category, count(*) FROM rrl_curated.reviews_enriched GROUP BY category
UNION ALL
SELECT 'rrl_curated.category_monthly', category, count(*) FROM rrl_curated.category_monthly GROUP BY category
UNION ALL
SELECT 'rrl_curated.category_daily', category, count(*) FROM rrl_curated.category_daily GROUP BY category
UNION ALL
SELECT 'rrl_curated.product_monthly', category, count(*) FROM rrl_curated.product_monthly GROUP BY category
ORDER BY table_name, category
