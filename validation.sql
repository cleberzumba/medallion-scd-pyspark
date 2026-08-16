-- =============================================================
-- validation.sql
-- =============================================================
-- Validation queries to run after the pipeline completes.
-- Execute in the Databricks SQL Editor or in a notebook cell
-- prefixed with %sql.
-- =============================================================

USE CATALOG medallion;
USE SCHEMA medallion_scd;


-- =============================================================
-- 1 — Row count across all layers
-- =============================================================
-- Expected: bronze 7 / 7 / 9, silver dims 4 / 6 / 7, fact 7
SELECT 'bronze_raw_customers'       AS table_name, COUNT(*) AS rows FROM bronze_raw_customers
UNION ALL SELECT 'bronze_raw_orders',              COUNT(*) FROM bronze_raw_orders
UNION ALL SELECT 'bronze_raw_products',            COUNT(*) FROM bronze_raw_products
UNION ALL SELECT 'silver_stg_customers',           COUNT(*) FROM silver_stg_customers
UNION ALL SELECT 'silver_stg_products',            COUNT(*) FROM silver_stg_products
UNION ALL SELECT 'silver_dim_customers_type1',     COUNT(*) FROM silver_dim_customers_type1
UNION ALL SELECT 'silver_dim_customers_type2',     COUNT(*) FROM silver_dim_customers_type2
UNION ALL SELECT 'silver_dim_products_type2',      COUNT(*) FROM silver_dim_products_type2
UNION ALL SELECT 'silver_fact_orders',             COUNT(*) FROM silver_fact_orders
UNION ALL SELECT 'gold_agg_sales_by_city',         COUNT(*) FROM gold_agg_sales_by_city
UNION ALL SELECT 'gold_agg_sales_by_category',     COUNT(*) FROM gold_agg_sales_by_category
UNION ALL SELECT 'gold_ranking_customers',         COUNT(*) FROM gold_ranking_customers
ORDER BY table_name;


-- =============================================================
-- 2 — SCD Type 1 (customers): only the current state
-- =============================================================
-- Expected: 4 rows, one per customer, no history.
-- John in Mountain View, Mary in Cupertino.
SELECT * FROM silver_dim_customers_type1 ORDER BY customer_id;


-- =============================================================
-- 3 — SCD Type 2 (customers): full address history
-- =============================================================
-- Expected: John and Mary with 2 rows each (old row closed,
-- new row current). Charles and Anna with 1 row each.
SELECT
    customer_id,
    name,
    city,
    valid_from,
    valid_to,
    is_current
FROM silver_dim_customers_type2
ORDER BY customer_id, valid_from;


-- =============================================================
-- 4 — SCD Type 2 (products): full price history
-- =============================================================
-- Expected: iPhone 15 with 2 rows (999.00 → 899.00),
-- T-Shirt with 2 rows (29.90 → 34.90), others with 1.
SELECT
    product_id,
    product_name,
    category,
    price,
    valid_from,
    valid_to,
    is_current
FROM silver_dim_products_type2
ORDER BY product_id, valid_from;


-- =============================================================
-- 5 — Integrity: exactly one current version per key
-- =============================================================
-- Expected: both queries return ZERO rows.
-- Any row here means the SCD merge is broken.
SELECT 'customers' AS dimension, customer_id AS key_value, COUNT(*) AS current_versions
FROM silver_dim_customers_type2
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1

UNION ALL

SELECT 'products', product_id, COUNT(*)
FROM silver_dim_products_type2
WHERE is_current = true
GROUP BY product_id
HAVING COUNT(*) > 1;


-- =============================================================
-- 6 — Integrity: current rows must have valid_to NULL
-- =============================================================
-- Expected: ZERO rows.
SELECT 'customers' AS dimension, customer_id AS key_value, valid_from, valid_to
FROM silver_dim_customers_type2
WHERE is_current = true AND valid_to IS NOT NULL

UNION ALL

SELECT 'products', product_id, valid_from, valid_to
FROM silver_dim_products_type2
WHERE is_current = true AND valid_to IS NOT NULL;


-- =============================================================
-- 7 — Referential integrity: no orphan orders
-- =============================================================
-- Expected: ZERO rows. Every order must reference an existing
-- customer and an existing product.
SELECT 'orphan customer_id' AS issue, f.order_id, CAST(f.customer_id AS STRING) AS missing_key
FROM silver_fact_orders f
LEFT ANTI JOIN silver_dim_customers_type2 d ON f.customer_id = d.customer_id

UNION ALL

SELECT 'orphan product_id', f.order_id, f.product_id
FROM silver_fact_orders f
LEFT ANTI JOIN silver_dim_products_type2 p ON f.product_id = p.product_id;


-- =============================================================
-- 8 — Point-in-time query: state of the world on a given date
-- =============================================================
-- The core benefit of SCD Type 2: reconstruct any past state.
-- On 2026-03-15 John still lived in San Francisco.
SELECT
    customer_id,
    name,
    city,
    valid_from,
    valid_to
FROM silver_dim_customers_type2
WHERE valid_from <= DATE('2026-03-15')
  AND (valid_to  >  DATE('2026-03-15') OR valid_to IS NULL)
ORDER BY customer_id;


-- =============================================================
-- 9 — Gold: business results
-- =============================================================
SELECT * FROM gold_agg_sales_by_city     ORDER BY total_amount DESC;

SELECT * FROM gold_agg_sales_by_category ORDER BY total_amount DESC;

SELECT * FROM gold_ranking_customers     ORDER BY ranking;


-- =============================================================
-- 10 — Ingestion traceability (Bronze metadata)
-- =============================================================
-- Confirms each row can be traced back to its source file.
SELECT
    _source_file,
    COUNT(*) AS rows_ingested,
    MIN(_ingestion_ts) AS ingested_at
FROM bronze_raw_customers
GROUP BY _source_file
ORDER BY _source_file;
