"""
etl_autoloader.py
-----------------
Production-grade Bronze ingestion using Databricks Auto Loader.

Simulates a realistic multi-source environment where different upstream systems
deliver data in different formats — a common pattern in enterprise pipelines.

Source formats:
    - Customers  → JSON     (simulates a CRM API / REST endpoint)
    - Orders     → Parquet  (simulates a CDC pipeline / Debezium / Fivetran)
    - Products   → CSV      (simulates a legacy ERP / mainframe export)

Difference from etl.py:
    - etl.py:            hardcoded data (didactic, runs anywhere)
    - etl_autoloader.py: real multi-format file ingestion from cloud storage

Features:
    - Auto Loader (cloudFiles) for incremental ingestion
    - Trigger AvailableNow (batch-style — runs once, ingests all new files, stops)
    - Unity Catalog three-level namespace (catalog.schema.table)
    - Schema evolution (auto-adds new columns)
    - Checkpointing (never re-processes the same file)
    - Format-agnostic helper function — easy to add new sources

Authentication:
    Handled by Unity Catalog through a Storage Credential (Azure Access
    Connector / Managed Identity) and an External Location registered for
    the ADLS Gen2 container. No credentials are needed in the code.

After this script runs, call the Silver → Gold transformations from etl.py.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, current_date, col


# =========================================================
# CONFIGURATION
# =========================================================

# Unity Catalog namespace (catalog.schema.table)
CATALOG = "medallion"
SCHEMA  = "medallion_scd"

# ─── Storage base path ─────────────────────────────
STORAGE_BASE = "abfss://medallion@medallionscdczs.dfs.core.windows.net"
# ────────────────────────────────────────────────────

# Landing paths — where the source systems drop files
CUSTOMERS_LANDING_PATH = f"{STORAGE_BASE}/raw/customers/"    # *.json
ORDERS_LANDING_PATH    = f"{STORAGE_BASE}/raw/orders/"       # *.parquet
PRODUCTS_LANDING_PATH  = f"{STORAGE_BASE}/raw/products/"     # *.csv

# Checkpoint paths — Auto Loader tracks which files were processed
CUSTOMERS_CHECKPOINT_PATH = f"{STORAGE_BASE}/_system/checkpoints/bronze_raw_customers/"
ORDERS_CHECKPOINT_PATH    = f"{STORAGE_BASE}/_system/checkpoints/bronze_raw_orders/"
PRODUCTS_CHECKPOINT_PATH  = f"{STORAGE_BASE}/_system/checkpoints/bronze_raw_products/"

# Schema paths — Auto Loader stores the inferred schema here
CUSTOMERS_SCHEMA_PATH = f"{STORAGE_BASE}/_system/schemas/customers/"
ORDERS_SCHEMA_PATH    = f"{STORAGE_BASE}/_system/schemas/orders/"
PRODUCTS_SCHEMA_PATH  = f"{STORAGE_BASE}/_system/schemas/products/"


def get_spark():
    """
    Returns the active SparkSession.

    Inside a Databricks notebook or job the session already exists, so this
    simply reuses it. Outside Databricks it creates a local one.
    """
    return SparkSession.builder.appName("Medallion-SCD-Autoloader").getOrCreate()


def setup_unity_catalog(spark):
    """
    Selects the target catalog and schema.

    Both are created by unity_catalog_setup.sql, which must run first. This
    script deliberately does NOT create them: a CREATE CATALOG here would omit
    the MANAGED LOCATION and silently place the catalog in the metastore root
    instead of ADLS, and IF NOT EXISTS would then prevent the setup script from
    ever fixing it. Failing loudly is the correct behaviour — especially here,
    since this script also runs unattended as a scheduled Job task.
    """
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
    print(f"Using Unity Catalog namespace: {CATALOG}.{SCHEMA}")


# =========================================================
# GENERIC AUTO LOADER FUNCTION
# =========================================================
def ingest_bronze_autoloader(
    spark,
    *,
    table_name: str,
    file_format: str,
    landing_path: str,
    checkpoint_path: str,
    schema_path: str,
    format_options: dict = None,
):
    """
    Generic Bronze ingestion via Auto Loader. Handles any supported file format.

    Args:
        table_name       : Fully qualified target table (catalog.schema.table).
        file_format      : "json", "parquet", "csv", "avro", "orc", "text".
        landing_path     : Cloud storage path where source files arrive.
        checkpoint_path  : Cloud storage path for streaming checkpoint state.
        schema_path      : Cloud storage path for inferred schema persistence.
        format_options   : Optional dict of format-specific reader options.
                          Example CSV: {"header": "true", "delimiter": ","}.

    Auto Loader options used:
        - cloudFiles.format             → file format to read
        - cloudFiles.schemaLocation     → where to persist inferred schema
        - cloudFiles.schemaEvolutionMode= addNewColumns → new columns added gracefully
        - cloudFiles.inferColumnTypes   → infers types beyond raw strings

    Writer options used:
        - checkpointLocation → tracks processed files (prevents duplicates)
        - mergeSchema        → allows schema evolution on the target Delta table
        - trigger(availableNow=True) → processes ALL current files, then stops
    """
    print(f"\n[BRONZE] Ingesting {file_format.upper()} from {landing_path}")
    print(f"         Target: {table_name}")

    # ── READ (streaming source: Auto Loader) ──────────
    reader = (spark.readStream
              .format("cloudFiles")
              .option("cloudFiles.format",              file_format)
              .option("cloudFiles.schemaLocation",      schema_path)
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
              .option("cloudFiles.inferColumnTypes",    "true"))

    # Apply format-specific options (ex: CSV header + delimiter)
    if format_options:
        for key, value in format_options.items():
            reader = reader.option(key, value)

    df = reader.load(landing_path)

    # ── ENRICH with ingestion metadata (traceability) ─
    df_bronze = (df
        .withColumn("_ingestion_ts",   current_timestamp())
        .withColumn("_ingestion_date", current_date())
        .withColumn("_source_file",    col("_metadata.file_path")))

    # ── WRITE (streaming sink: Delta + Unity Catalog) ─
    query = (df_bronze.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .option("mergeSchema", "true")
             .trigger(availableNow=True)
             .toTable(table_name))

    query.awaitTermination()
    print("         Ingestion completed.")


# =========================================================
# MAIN
# =========================================================
def main():
    """Ingests every source system into its Bronze table."""
    spark = get_spark()
    setup_unity_catalog(spark)

    print("=" * 60)
    print("BRONZE INGESTION VIA AUTO LOADER (multi-format)")
    print("=" * 60)

    # ─────────────────────────────────────────────
    # JSON — Customers (from CRM API / REST endpoint)
    # ─────────────────────────────────────────────
    ingest_bronze_autoloader(
        spark,
        table_name      = f"{CATALOG}.{SCHEMA}.bronze_raw_customers",
        file_format     = "json",
        landing_path    = CUSTOMERS_LANDING_PATH,
        checkpoint_path = CUSTOMERS_CHECKPOINT_PATH,
        schema_path     = CUSTOMERS_SCHEMA_PATH,
    )

    # ─────────────────────────────────────────────
    # PARQUET — Orders (from CDC / Debezium / Fivetran)
    # ─────────────────────────────────────────────
    ingest_bronze_autoloader(
        spark,
        table_name      = f"{CATALOG}.{SCHEMA}.bronze_raw_orders",
        file_format     = "parquet",
        landing_path    = ORDERS_LANDING_PATH,
        checkpoint_path = ORDERS_CHECKPOINT_PATH,
        schema_path     = ORDERS_SCHEMA_PATH,
    )

    # ─────────────────────────────────────────────
    # CSV — Products (from legacy ERP / mainframe export)
    # ─────────────────────────────────────────────
    ingest_bronze_autoloader(
        spark,
        table_name      = f"{CATALOG}.{SCHEMA}.bronze_raw_products",
        file_format     = "csv",
        landing_path    = PRODUCTS_LANDING_PATH,
        checkpoint_path = PRODUCTS_CHECKPOINT_PATH,
        schema_path     = PRODUCTS_SCHEMA_PATH,
        format_options  = {
            "header":    "true",   # first row is header
            "delimiter": ",",      # comma-separated
        },
    )

    print("\nBronze ingestion completed for all sources.")
    print("Next step: run etl.py for the Silver and Gold transformations.")


main()
