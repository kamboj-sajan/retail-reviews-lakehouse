-- Rows in every warehouse table, per category where the table has one.
-- Compare with sql/checks/table_counts.sql (Athena, the lake): fact_review
-- must equal rrl_curated.reviews_enriched and dim_product rrl_cleaned.products.
SELECT 'dw.fact_review' AS table_name, category, count(*) AS row_count FROM dw.fact_review GROUP BY category
UNION ALL
SELECT 'dw.dim_product', category, count(*) FROM dw.dim_product GROUP BY category
UNION ALL
SELECT 'dw.dim_date', min(year_month) || ' to ' || max(year_month), count(*) FROM dw.dim_date
ORDER BY 1, 2
