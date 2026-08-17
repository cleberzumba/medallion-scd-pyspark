"""
create_tables.py
----------------
Creates the Delta tables of the 3 Medallion layers as EXTERNAL TABLES
pointing to explicit paths in Azure Data Lake Storage Gen2.

Storage layout:

    <STORAGE_BASE>/
    ├── raw/                       ← source files (input)
    ├── lakehouse/
    │   ├── bronze/                ← Bronze tables
    │   ├── silver/                ← Silver tables
    │   └── gold/                  ← Gold tables
    └── _system/                   ← Auto Loader internals + catalog

Why external tables?
    - Explicit control over physical location
    - Portability across workspaces
    - Compliance and auditability
    - Standard in enterprise Databricks deployments

Authentication:
    Handled by Unity Catalog through a Storage Credential (Azure Access
    Connector / Managed Identity) and an External Location registered for
    the ADLS Gen2 container. No credentials are needed in the code.

Tables created:
    BRONZE
       ├─ bronze_raw_customers
       ├─ bronze_raw_orders
       └─ bronze_raw_products
    SILVER
       ├─ silver_stg_customers
       ├─ silver_stg_products
       ├─ silver_dim_customers_type1  (SCD Type 1)
       ├─ silver_dim_customers_type2  (SCD Type 2)
       ├─ silver_dim_products_type2   (SCD Type 2 — price versioning)
       └─ silver_fact_orders
    GOLD
       ├─ gold_agg_sales_by_city
       ├─ gold_agg_sales_by_category
       └─ gold_ranking_customers
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType,
    BooleanType, TimestampType, DoubleType
)


# =========================================================
# CONFIGURATION
# =========================================================

# Unity Catalog namespace
CATALOG = "medallion"
SCHEMA  = "medallion_scd"

# ─── Storage base path ─────────────────────────────
STORAGE_BASE = "abfss://medallion@medallionscdczs.dfs.core.windows.net"
# ────────────────────────────────────────────────────

# Paths by medallion layer
BRONZE_BASE = f"{STORAGE_BASE}/lakehouse/bronze"
SILVER_BASE = f"{STORAGE_BASE}/lakehouse/silver"
GOLD_BASE   = f"{STORAGE_BASE}/lakehouse/gold"


def get_spark():
    """
    Returns the active SparkSession.

    Inside a Databricks notebook or job the session already exists, so this
    simply reuses it. Outside Databricks it creates a local one.
    """
    return SparkSession.builder.appName("Medallion-SCD-CreateTables").getOrCreate()


def setup_unity_catalog(spark):
    """
    Selects the target catalog and schema.

    Both are created by unity_catalog_setup.sql, which must run first. This
    script deliberately does NOT create them: a CREATE CATALOG here would omit
    the MANAGED LOCATION and silently place the catalog in the metastore root
    instead of ADLS, and IF NOT EXISTS would then prevent the setup script from
    ever fixing it. Failing loudly is the correct behaviour.
    """
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
    print(f"Using namespace: {CATALOG}.{SCHEMA}")


# =========================================================
# BRONZE — Raw ingestion
# =========================================================
def create_bronze_customers(spark):
    """Bronze: raw customer data (arrives as JSON from a CRM API)."""
    schema = StructType([
        StructField("customer_id",     StringType(),    nullable=True),
        StructField("name",            StringType(),    nullable=True),
        StructField("city",            StringType(),    nullable=True),
        StructField("event_date",      StringType(),    nullable=True),
        StructField("_ingestion_ts",   TimestampType(), nullable=False),
        StructField("_ingestion_date", DateType(),      nullable=False),
        StructField("_source_file",    StringType(),    nullable=True),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .partitionBy("_ingestion_date")
          .option("path", f"{BRONZE_BASE}/bronze_raw_customers")
          .saveAsTable("bronze_raw_customers"))

    print(f"bronze_raw_customers created at {BRONZE_BASE}/bronze_raw_customers")


def create_bronze_orders(spark):
    """Bronze: raw order data (arrives as Parquet from a CDC pipeline)."""
    schema = StructType([
        StructField("order_id",        StringType(),    nullable=True),
        StructField("customer_id",     StringType(),    nullable=True),
        StructField("product_id",      StringType(),    nullable=True),
        StructField("amount",          StringType(),    nullable=True),
        StructField("order_date",      StringType(),    nullable=True),
        StructField("_ingestion_ts",   TimestampType(), nullable=False),
        StructField("_ingestion_date", DateType(),      nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .partitionBy("_ingestion_date")
          .option("path", f"{BRONZE_BASE}/bronze_raw_orders")
          .saveAsTable("bronze_raw_orders"))

    print(f"bronze_raw_orders created at {BRONZE_BASE}/bronze_raw_orders")


def create_bronze_products(spark):
    """Bronze: raw product data (arrives as CSV from a legacy ERP)."""
    schema = StructType([
        StructField("product_id",      StringType(),    nullable=True),
        StructField("product_name",    StringType(),    nullable=True),
        StructField("category",        StringType(),    nullable=True),
        StructField("price",           StringType(),    nullable=True),
        StructField("launch_date",     StringType(),    nullable=True),
        StructField("_ingestion_ts",   TimestampType(), nullable=False),
        StructField("_ingestion_date", DateType(),      nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .partitionBy("_ingestion_date")
          .option("path", f"{BRONZE_BASE}/bronze_raw_products")
          .saveAsTable("bronze_raw_products"))

    print(f"bronze_raw_products created at {BRONZE_BASE}/bronze_raw_products")


# =========================================================
# SILVER — Cleansed data + SCD dimensions + fact
# =========================================================
def create_silver_staging(spark):
    """Silver staging: cleaned and typed customer data."""
    schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("name",        StringType(),  nullable=False),
        StructField("city",        StringType(),  nullable=False),
        StructField("event_date",  DateType(),    nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{SILVER_BASE}/silver_stg_customers")
          .saveAsTable("silver_stg_customers"))

    print(f"silver_stg_customers created at {SILVER_BASE}/silver_stg_customers")


def create_silver_dim_type1(spark):
    """Silver: SCD Type 1 customer dimension (overwrite, no history)."""
    schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("name",        StringType(),  nullable=False),
        StructField("city",        StringType(),  nullable=False),
        StructField("update_date", DateType(),    nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{SILVER_BASE}/silver_dim_customers_type1")
          .saveAsTable("silver_dim_customers_type1"))

    print(f"silver_dim_customers_type1 created at {SILVER_BASE}/silver_dim_customers_type1")


def create_silver_dim_type2(spark):
    """Silver: SCD Type 2 customer dimension (versioned address history)."""
    schema = StructType([
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("name",        StringType(),  nullable=False),
        StructField("city",        StringType(),  nullable=False),
        StructField("valid_from",  DateType(),    nullable=False),
        StructField("valid_to",    DateType(),    nullable=True),
        StructField("is_current",  BooleanType(), nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{SILVER_BASE}/silver_dim_customers_type2")
          .saveAsTable("silver_dim_customers_type2"))

    print(f"silver_dim_customers_type2 created at {SILVER_BASE}/silver_dim_customers_type2")


def create_silver_stg_products(spark):
    """Silver staging: cleaned and typed product data."""
    schema = StructType([
        StructField("product_id",   StringType(), nullable=False),
        StructField("product_name", StringType(), nullable=False),
        StructField("category",     StringType(), nullable=False),
        StructField("price",        DoubleType(), nullable=False),
        StructField("launch_date",  DateType(),   nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{SILVER_BASE}/silver_stg_products")
          .saveAsTable("silver_stg_products"))

    print(f"silver_stg_products created at {SILVER_BASE}/silver_stg_products")


def create_silver_dim_products_type2(spark):
    """
    Silver: SCD Type 2 product dimension (price history).

    Real-world use case: a product's price changes over time, and historical
    orders must be joined against the price valid on the day they were placed.
    """
    schema = StructType([
        StructField("product_id",   StringType(),  nullable=False),
        StructField("product_name", StringType(),  nullable=False),
        StructField("category",     StringType(),  nullable=False),
        StructField("price",        DoubleType(),  nullable=False),
        StructField("valid_from",   DateType(),    nullable=False),
        StructField("valid_to",     DateType(),    nullable=True),
        StructField("is_current",   BooleanType(), nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{SILVER_BASE}/silver_dim_products_type2")
          .saveAsTable("silver_dim_products_type2"))

    print(f"silver_dim_products_type2 created at {SILVER_BASE}/silver_dim_products_type2")


def create_silver_fact_orders(spark):
    """Silver: orders fact table, partitioned by year and month."""
    schema = StructType([
        StructField("order_id",    IntegerType(), nullable=False),
        StructField("customer_id", IntegerType(), nullable=False),
        StructField("product_id",  StringType(),  nullable=False),
        StructField("amount",      DoubleType(),  nullable=False),
        StructField("order_date",  DateType(),    nullable=False),
        StructField("year",        IntegerType(), nullable=False),
        StructField("month",       IntegerType(), nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .partitionBy("year", "month")
          .option("path", f"{SILVER_BASE}/silver_fact_orders")
          .saveAsTable("silver_fact_orders"))

    print(f"silver_fact_orders created at {SILVER_BASE}/silver_fact_orders")


# =========================================================
# GOLD — Business aggregations
# =========================================================
def create_gold_agg_sales(spark):
    """Gold: sales aggregated by customer city."""
    schema = StructType([
        StructField("city",         StringType(),  nullable=False),
        StructField("total_orders", IntegerType(), nullable=False),
        StructField("total_amount", DoubleType(),  nullable=False),
        StructField("update_date",  DateType(),    nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{GOLD_BASE}/gold_agg_sales_by_city")
          .saveAsTable("gold_agg_sales_by_city"))

    print(f"gold_agg_sales_by_city created at {GOLD_BASE}/gold_agg_sales_by_city")


def create_gold_agg_sales_by_category(spark):
    """Gold: sales aggregated by product category."""
    schema = StructType([
        StructField("category",        StringType(),  nullable=False),
        StructField("total_orders",    IntegerType(), nullable=False),
        StructField("total_amount",    DoubleType(),  nullable=False),
        StructField("unique_products", IntegerType(), nullable=False),
        StructField("update_date",     DateType(),    nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{GOLD_BASE}/gold_agg_sales_by_category")
          .saveAsTable("gold_agg_sales_by_category"))

    print(f"gold_agg_sales_by_category created at {GOLD_BASE}/gold_agg_sales_by_category")


def create_gold_ranking(spark):
    """Gold: top customers ranked by total spend."""
    schema = StructType([
        StructField("customer_id",  IntegerType(), nullable=False),
        StructField("name",         StringType(),  nullable=False),
        StructField("city",         StringType(),  nullable=False),
        StructField("total_amount", DoubleType(),  nullable=False),
        StructField("ranking",      IntegerType(), nullable=False),
    ])

    (spark.createDataFrame([], schema=schema)
          .write
          .format("delta")
          .mode("overwrite")
          .option("path", f"{GOLD_BASE}/gold_ranking_customers")
          .saveAsTable("gold_ranking_customers"))

    print(f"gold_ranking_customers created at {GOLD_BASE}/gold_ranking_customers")


# =========================================================
# MAIN
# =========================================================
def main():
    """Creates every table of the three medallion layers."""
    spark = get_spark()
    setup_unity_catalog(spark)

    print(f"\nCreating external Delta tables under {STORAGE_BASE}/lakehouse/ ...\n")

    # Bronze
    create_bronze_customers(spark)
    create_bronze_orders(spark)
    create_bronze_products(spark)

    # Silver
    create_silver_staging(spark)
    create_silver_dim_type1(spark)
    create_silver_dim_type2(spark)
    create_silver_stg_products(spark)
    create_silver_dim_products_type2(spark)
    create_silver_fact_orders(spark)

    # Gold
    create_gold_agg_sales(spark)
    create_gold_agg_sales_by_category(spark)
    create_gold_ranking(spark)

    print("\nAll external tables have been created.")
    print("Next step: run etl_autoloader.py to ingest data into Bronze.")


main()
