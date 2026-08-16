-- =============================================================
-- unity_catalog_setup.sql
-- =============================================================
-- Unity Catalog governance setup for the Medallion + SCD project.
--
-- Demonstrates:
--   - Catalog & Schema creation with properties
--   - Table comments (documentation as metadata)
--   - Tags for classification (layer, sensitivity, domain)
--   - Column-level tags for PII / sensitive data
--   - GRANTS with role-based access control (RBAC)
--   - Column masking (PII protection)
--   - Row-level security (data filtering by user context)
--   - Verification queries
--
-- Prerequisites:
--   - Databricks workspace with Unity Catalog enabled
--   - User with METASTORE ADMIN or workspace admin role
--   - Groups already created in the account: data_engineers,
--     data_analysts, data_scientists (see comments below)
-- =============================================================


-- =============================================================
-- SECTION 1 — Prerequisites (managed at account level)
-- =============================================================
-- Groups should be created via the Databricks account console:
--   Account Admin → User Management → Groups
--
-- Required groups for this project:
--   - data_engineers   → full access to all layers
--   - data_analysts    → read access to gold (BI consumption)
--   - data_scientists  → read access to silver + gold (feature engineering)
--
-- The commands below assume these groups exist.


-- =============================================================
-- SECTION 2 — Catalog & Schema creation
-- =============================================================

-- Create the catalog (top-level namespace).
-- MANAGED LOCATION points to the ADLS Gen2 container registered as an
-- External Location in Unity Catalog. Any managed table created in this
-- catalog will be physically stored there.
CREATE CATALOG IF NOT EXISTS medallion
  MANAGED LOCATION 'abfss://medallion@medallionscdczs.dfs.core.windows.net/_system/catalog'
  COMMENT 'Catalog for the Medallion + SCD Lakehouse project';

-- Create the schema for this project
CREATE SCHEMA IF NOT EXISTS medallion.medallion_scd
  COMMENT 'Medallion architecture (bronze/silver/gold) with SCD Type 1 and Type 2 dimensions.'
  WITH DBPROPERTIES (
    'project'      = 'medallion-scd-pyspark',
    'owner'        = 'data_engineering_team',
    'environment'  = 'production',
    'created_by'   = 'Cleber Zumba Souza'
  );

USE CATALOG medallion;
USE SCHEMA medallion_scd;


-- =============================================================
-- SECTION 3 — Table comments (self-documenting data catalog)
-- =============================================================

COMMENT ON TABLE bronze_raw_customers IS
  'Raw customer data as received from CRM API (JSON). Partitioned by ingestion date. Never modified — source of truth for reprocessing.';

COMMENT ON TABLE bronze_raw_orders IS
  'Raw order data from CDC pipeline (Parquet). Contains customer_id and product_id FKs.';

COMMENT ON TABLE bronze_raw_products IS
  'Raw product catalog from legacy ERP export (CSV).';

COMMENT ON TABLE silver_stg_customers IS
  'Cleaned and typed customer staging table. Feeds SCD Type 1 and Type 2 dimensions.';

COMMENT ON TABLE silver_stg_products IS
  'Cleaned and typed product staging table. Feeds the SCD Type 2 product dimension.';

COMMENT ON TABLE silver_dim_customers_type1 IS
  'Customer dimension — SCD Type 1 (overwrite). Reflects only the CURRENT state of each customer. No history preserved.';

COMMENT ON TABLE silver_dim_customers_type2 IS
  'Customer dimension — SCD Type 2 (versioned). Preserves full history of address changes via valid_from / valid_to / is_current.';

COMMENT ON TABLE silver_dim_products_type2 IS
  'Product dimension — SCD Type 2 (versioned). Tracks price changes over time to support point-in-time joins with fact_orders.';

COMMENT ON TABLE silver_fact_orders IS
  'Orders fact table. Partitioned by year, month. FKs to customers and products dimensions.';

COMMENT ON TABLE gold_agg_sales_by_city IS
  'Sales aggregation by customer city. Consumed by BI dashboards.';

COMMENT ON TABLE gold_agg_sales_by_category IS
  'Sales aggregation by product category. Consumed by BI dashboards.';

COMMENT ON TABLE gold_ranking_customers IS
  'Top customers ranked by total spend (window function). Refreshed daily.';


-- =============================================================
-- SECTION 4 — Tags for classification
-- =============================================================
-- Tags allow filtering, searching, and applying policies at scale.
-- Databricks convention: use lowercase snake_case for keys.

-- Layer tags (bronze / silver / gold)
ALTER TABLE bronze_raw_customers        SET TAGS ('layer' = 'bronze', 'source_format' = 'json',    'domain' = 'customer');
ALTER TABLE bronze_raw_orders           SET TAGS ('layer' = 'bronze', 'source_format' = 'parquet', 'domain' = 'sales');
ALTER TABLE bronze_raw_products         SET TAGS ('layer' = 'bronze', 'source_format' = 'csv',     'domain' = 'catalog');

ALTER TABLE silver_stg_customers        SET TAGS ('layer' = 'silver', 'stage'  = 'staging', 'domain' = 'customer');
ALTER TABLE silver_stg_products         SET TAGS ('layer' = 'silver', 'stage'  = 'staging', 'domain' = 'catalog');
ALTER TABLE silver_dim_customers_type1  SET TAGS ('layer' = 'silver', 'scd_type' = 'type_1', 'domain' = 'customer');
ALTER TABLE silver_dim_customers_type2  SET TAGS ('layer' = 'silver', 'scd_type' = 'type_2', 'domain' = 'customer');
ALTER TABLE silver_dim_products_type2   SET TAGS ('layer' = 'silver', 'scd_type' = 'type_2', 'domain' = 'catalog');
ALTER TABLE silver_fact_orders          SET TAGS ('layer' = 'silver', 'type' = 'fact',       'domain' = 'sales');

ALTER TABLE gold_agg_sales_by_city      SET TAGS ('layer' = 'gold', 'type' = 'aggregation', 'domain' = 'sales');
ALTER TABLE gold_agg_sales_by_category  SET TAGS ('layer' = 'gold', 'type' = 'aggregation', 'domain' = 'sales');
ALTER TABLE gold_ranking_customers      SET TAGS ('layer' = 'gold', 'type' = 'ranking',     'domain' = 'customer');

-- Sensitivity tags (for PII compliance — LGPD, GDPR)
ALTER TABLE bronze_raw_customers       SET TAGS ('contains_pii' = 'true');
ALTER TABLE silver_stg_customers       SET TAGS ('contains_pii' = 'true');
ALTER TABLE silver_dim_customers_type1 SET TAGS ('contains_pii' = 'true');
ALTER TABLE silver_dim_customers_type2 SET TAGS ('contains_pii' = 'true');
ALTER TABLE gold_ranking_customers     SET TAGS ('contains_pii' = 'true');


-- =============================================================
-- SECTION 5 — Column-level tags (PII identification)
-- =============================================================
-- Marking specific columns as PII enables:
--   - Automated discovery via `SHOW TAGS`
--   - Application of column masking policies
--   - Compliance reports (LGPD, GDPR)

ALTER TABLE silver_dim_customers_type2 ALTER COLUMN name SET TAGS ('pii' = 'true', 'classification' = 'personal');
ALTER TABLE silver_dim_customers_type2 ALTER COLUMN city SET TAGS ('pii' = 'true', 'classification' = 'location');

ALTER TABLE silver_dim_customers_type1 ALTER COLUMN name SET TAGS ('pii' = 'true', 'classification' = 'personal');
ALTER TABLE silver_dim_customers_type1 ALTER COLUMN city SET TAGS ('pii' = 'true', 'classification' = 'location');

ALTER TABLE gold_ranking_customers ALTER COLUMN name SET TAGS ('pii' = 'true', 'classification' = 'personal');
ALTER TABLE gold_ranking_customers ALTER COLUMN city SET TAGS ('pii' = 'true', 'classification' = 'location');

-- Financial tags on amount columns
ALTER TABLE silver_fact_orders          ALTER COLUMN amount       SET TAGS ('classification' = 'financial');
ALTER TABLE gold_agg_sales_by_city      ALTER COLUMN total_amount SET TAGS ('classification' = 'financial');
ALTER TABLE gold_agg_sales_by_category  ALTER COLUMN total_amount SET TAGS ('classification' = 'financial');
ALTER TABLE gold_ranking_customers      ALTER COLUMN total_amount SET TAGS ('classification' = 'financial');


-- =============================================================
-- SECTION 6 — GRANTS (Role-Based Access Control)
-- =============================================================

-- ── data_engineers: FULL ACCESS to everything ──────────────
-- Own the pipeline and need to create/drop tables.
GRANT USE CATALOG          ON CATALOG medallion                       TO `data_engineers`;
GRANT USE SCHEMA           ON SCHEMA  medallion.medallion_scd         TO `data_engineers`;
GRANT ALL PRIVILEGES       ON SCHEMA  medallion.medallion_scd         TO `data_engineers`;


-- ── data_analysts: READ ONLY on gold layer ─────────────────
-- Consume gold tables for BI dashboards and reports.
GRANT USE CATALOG          ON CATALOG medallion                       TO `data_analysts`;
GRANT USE SCHEMA           ON SCHEMA  medallion.medallion_scd         TO `data_analysts`;

GRANT SELECT ON TABLE medallion.medallion_scd.gold_agg_sales_by_city     TO `data_analysts`;
GRANT SELECT ON TABLE medallion.medallion_scd.gold_agg_sales_by_category TO `data_analysts`;
GRANT SELECT ON TABLE medallion.medallion_scd.gold_ranking_customers     TO `data_analysts`;


-- ── data_scientists: READ ONLY on silver + gold ────────────
-- Access silver for feature engineering, gold for validation.
GRANT USE CATALOG          ON CATALOG medallion                       TO `data_scientists`;
GRANT USE SCHEMA           ON SCHEMA  medallion.medallion_scd         TO `data_scientists`;

GRANT SELECT ON TABLE medallion.medallion_scd.silver_stg_customers        TO `data_scientists`;
GRANT SELECT ON TABLE medallion.medallion_scd.silver_stg_products         TO `data_scientists`;
GRANT SELECT ON TABLE medallion.medallion_scd.silver_dim_customers_type2  TO `data_scientists`;
GRANT SELECT ON TABLE medallion.medallion_scd.silver_dim_products_type2   TO `data_scientists`;
GRANT SELECT ON TABLE medallion.medallion_scd.silver_fact_orders          TO `data_scientists`;

GRANT SELECT ON TABLE medallion.medallion_scd.gold_agg_sales_by_city     TO `data_scientists`;
GRANT SELECT ON TABLE medallion.medallion_scd.gold_agg_sales_by_category TO `data_scientists`;
GRANT SELECT ON TABLE medallion.medallion_scd.gold_ranking_customers     TO `data_scientists`;

-- IMPORTANT: bronze layer is NEVER exposed to analysts or scientists.
-- Only the pipeline (data_engineers) can read it directly.


-- =============================================================
-- SECTION 7 — Column masking (PII protection)
-- =============================================================
-- Column masking hides sensitive data from non-privileged users.
-- Analysts see 'XXXX', engineers see the real value.

-- Create the mask function (returns masked or real value based on group)
CREATE OR REPLACE FUNCTION medallion.medallion_scd.mask_pii_name(name STRING)
RETURN CASE
  WHEN is_account_group_member('data_engineers') THEN name
  ELSE 'XXXX'
END;

-- Apply the mask to the name column
ALTER TABLE medallion.medallion_scd.silver_dim_customers_type2
  ALTER COLUMN name SET MASK medallion.medallion_scd.mask_pii_name;

ALTER TABLE medallion.medallion_scd.gold_ranking_customers
  ALTER COLUMN name SET MASK medallion.medallion_scd.mask_pii_name;

-- Behavior after applying the mask:
--   data_engineers   → sees 'John', 'Mary', 'Charles', 'Anna'
--   data_analysts    → sees 'XXXX', 'XXXX', 'XXXX', 'XXXX'
--   data_scientists  → sees 'XXXX', 'XXXX', 'XXXX', 'XXXX'
 

-- =============================================================
-- SECTION 8 — Row-level security (data filtering)
-- =============================================================
-- Row filters restrict which rows a user can see based on their identity.
-- Example: analysts can only see current customers (is_current = true),
-- not the full historical audit trail.

CREATE OR REPLACE FUNCTION medallion.medallion_scd.filter_current_only(is_current BOOLEAN)
RETURN CASE
  WHEN is_account_group_member('data_engineers') THEN true    -- engineers see everything
  ELSE is_current                                             -- others see only current
END;

-- Apply the filter to the SCD Type 2 table
ALTER TABLE medallion.medallion_scd.silver_dim_customers_type2
  SET ROW FILTER medallion.medallion_scd.filter_current_only ON (is_current);

ALTER TABLE medallion.medallion_scd.silver_dim_products_type2
  SET ROW FILTER medallion.medallion_scd.filter_current_only ON (is_current);


-- =============================================================
-- SECTION 9 — Verification queries
-- =============================================================
-- Run these to validate the setup.

-- Check catalog properties
DESCRIBE CATALOG EXTENDED medallion;

-- Check schema properties
DESCRIBE SCHEMA EXTENDED medallion.medallion_scd;

-- List all tables and their tags
SHOW TABLES IN medallion.medallion_scd;

-- Show tags on a specific table
SHOW TAGS ON TABLE medallion.medallion_scd.silver_dim_customers_type2;

-- Show tags on a column
SHOW TAGS ON COLUMN medallion.medallion_scd.silver_dim_customers_type2.name;

-- List all grants on the schema
SHOW GRANTS ON SCHEMA medallion.medallion_scd;

-- List all grants on a specific table
SHOW GRANTS ON TABLE medallion.medallion_scd.gold_ranking_customers;

-- Show masks and row filters
DESCRIBE TABLE EXTENDED medallion.medallion_scd.silver_dim_customers_type2;


-- =============================================================
-- SECTION 10 — Cleanup (only if needed)
-- =============================================================
-- To reverse the changes above, uncomment and run:
--
-- ALTER TABLE medallion.medallion_scd.silver_dim_customers_type2 ALTER COLUMN name DROP MASK;
-- ALTER TABLE medallion.medallion_scd.gold_ranking_customers      ALTER COLUMN name DROP MASK;
-- ALTER TABLE medallion.medallion_scd.silver_dim_customers_type2  DROP ROW FILTER;
-- ALTER TABLE medallion.medallion_scd.silver_dim_products_type2   DROP ROW FILTER;
--
-- DROP FUNCTION IF EXISTS medallion.medallion_scd.mask_pii_name;
-- DROP FUNCTION IF EXISTS medallion.medallion_scd.filter_current_only;
--
-- REVOKE ALL PRIVILEGES ON SCHEMA medallion.medallion_scd FROM `data_engineers`;
-- REVOKE ALL PRIVILEGES ON SCHEMA medallion.medallion_scd FROM `data_analysts`;
-- REVOKE ALL PRIVILEGES ON SCHEMA medallion.medallion_scd FROM `data_scientists`;
--
-- DROP SCHEMA medallion.medallion_scd CASCADE;
