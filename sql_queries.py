"""
sql_queries.py
--------------
Query demonstrations over the Medallion layers.

Structure:
    BRONZE           — inspection of raw data
    SILVER           — SCD Type 1 and Type 2 dimensions
    GOLD             — consumption of aggregations
    JOIN TYPES       — inner, left, right, outer, cross, left_anti, left_semi
    SET OPERATIONS   — union, unionAll, distinct
    TIME TRAVEL      — reading a previous version of a Delta table

For integrity checks (row counts, orphan detection, one-current-version
assertions), see validation.sql instead — this file is about showing how
each read operation behaves.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# =========================================================
# CONFIGURATION
# =========================================================
CATALOG = "medallion"
SCHEMA  = "medallion_scd"


def get_spark():
    """
    Returns the active SparkSession.

    Inside a Databricks notebook or job the session already exists, so this
    simply reuses it. Outside Databricks it creates a local one.
    """
    return SparkSession.builder.appName("Medallion-SCD-Queries").getOrCreate()


def setup_unity_catalog(spark):
    """Selects the target catalog and schema."""
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
    print(f"Using namespace: {CATALOG}.{SCHEMA}")


# =========================================================
# BRONZE
# =========================================================
def query_bronze(spark):
    """Bronze: raw ingested data with traceability metadata."""
    print("\n" + "=" * 60)
    print("BRONZE — Raw customer data")
    print("=" * 60)

    (spark.read.table("bronze_raw_customers")
          .select("customer_id", "name", "city",
                  "event_date", "_source_file", "_ingestion_ts")
          .orderBy("_ingestion_ts", "customer_id")
          .show(truncate=False))


# =========================================================
# SILVER — Customers
# =========================================================
def query_silver_dim_type2_current(spark):
    """SCD Type 2: current version only."""
    print("\n" + "=" * 60)
    print("SILVER — SCD Type 2 customers (current version)")
    print("=" * 60)

    (spark.read.table("silver_dim_customers_type2")
          .filter(col("is_current") == True)
          .orderBy("customer_id")
          .show(truncate=False))


def query_silver_dim_type2_history(spark):
    """SCD Type 2: complete version history."""
    print("\n" + "=" * 60)
    print("SILVER — SCD Type 2 customers (complete history)")
    print("=" * 60)

    (spark.read.table("silver_dim_customers_type2")
          .orderBy("customer_id", "valid_from")
          .show(truncate=False))


def query_silver_point_in_time(spark, ref_date="2026-03-15"):
    """
    Point-in-time query — the core benefit of SCD Type 2.

    Reconstructs the state of every customer on a given date by selecting the
    version whose validity interval contains that date.
    """
    print("\n" + "=" * 60)
    print(f"SILVER — Customer state on {ref_date}")
    print("=" * 60)

    spark.sql(f"""
        SELECT customer_id, name, city, valid_from, valid_to
        FROM silver_dim_customers_type2
        WHERE valid_from <= DATE('{ref_date}')
          AND (valid_to > DATE('{ref_date}') OR valid_to IS NULL)
        ORDER BY customer_id
    """).show(truncate=False)


# =========================================================
# SILVER — Products
# =========================================================
def query_products_current(spark):
    """Products: current price of each item."""
    print("\n" + "=" * 60)
    print("SILVER — SCD Type 2 products (current version)")
    print("=" * 60)

    (spark.read.table("silver_dim_products_type2")
          .filter(col("is_current") == True)
          .orderBy("product_id")
          .show(truncate=False))


def query_products_history(spark):
    """Products: complete price history."""
    print("\n" + "=" * 60)
    print("SILVER — SCD Type 2 products (price history)")
    print("=" * 60)

    (spark.read.table("silver_dim_products_type2")
          .orderBy("product_id", "valid_from")
          .show(truncate=False))


# =========================================================
# GOLD
# =========================================================
def query_gold_sales(spark):
    """Gold: sales by customer city."""
    print("\n" + "=" * 60)
    print("GOLD — Sales by city")
    print("=" * 60)

    (spark.read.table("gold_agg_sales_by_city")
          .orderBy(col("total_amount").desc())
          .show(truncate=False))


def query_gold_sales_by_category(spark):
    """Gold: sales by product category."""
    print("\n" + "=" * 60)
    print("GOLD — Sales by category")
    print("=" * 60)

    (spark.read.table("gold_agg_sales_by_category")
          .orderBy(col("total_amount").desc())
          .show(truncate=False))


def query_gold_ranking(spark):
    """Gold: customer ranking (top 10)."""
    print("\n" + "=" * 60)
    print("GOLD — Customer ranking (top 10)")
    print("=" * 60)

    (spark.read.table("gold_ranking_customers")
          .orderBy("ranking")
          .limit(10)
          .show(truncate=False))


# =========================================================
# JOIN TYPES
# =========================================================
def demonstrate_join_types(spark):
    """
    Demonstrates every JOIN type available in PySpark.

    Uses the current customer dimension as the left side and the orders fact
    table as the right side.
    """
    print("\n" + "=" * 60)
    print("JOIN TYPES")
    print("=" * 60)

    dim = (spark.read.table("silver_dim_customers_type2")
                     .filter(col("is_current") == True)
                     .select("customer_id", "name", "city"))

    fact = (spark.read.table("silver_fact_orders")
                      .select("order_id", "customer_id", "amount"))

    # INNER — only rows matching on both sides
    print("\nINNER JOIN — customers who HAVE orders")
    (dim.join(fact, "customer_id", "inner")
        .orderBy("customer_id", "order_id")
        .show(truncate=False))

    # LEFT — every row from the left, matched or null
    print("\nLEFT JOIN — all customers, with their orders if any")
    (dim.join(fact, "customer_id", "left")
        .orderBy("customer_id")
        .show(truncate=False))

    # RIGHT — every row from the right, matched or null
    print("\nRIGHT JOIN — all orders, with the customer if present")
    (dim.join(fact, "customer_id", "right")
        .orderBy("customer_id")
        .show(truncate=False))

    # FULL OUTER — everything from both sides
    print("\nFULL OUTER JOIN — both sides, nulls where there is no match")
    (dim.join(fact, "customer_id", "outer")
        .orderBy("customer_id")
        .show(truncate=False))

    # LEFT ANTI — left rows WITHOUT a match (orphan detection)
    print("\nLEFT ANTI JOIN — customers who have NO orders")
    (dim.join(fact, "customer_id", "left_anti")
        .orderBy("customer_id")
        .show(truncate=False))

    # LEFT SEMI — left rows WITH a match, no columns from the right
    print("\nLEFT SEMI JOIN — customers who have orders (left columns only)")
    (dim.join(fact, "customer_id", "left_semi")
        .orderBy("customer_id")
        .show(truncate=False))

    # CROSS — cartesian product; dangerous at scale, limited here
    print("\nCROSS JOIN — cartesian product (limited to 2 x 3)")
    (dim.limit(2)
        .crossJoin(fact.limit(3))
        .show(truncate=False))


# =========================================================
# SET OPERATIONS
# =========================================================
def demonstrate_unions(spark):
    """
    Demonstrates:
        - union()      → concatenates by column POSITION
        - unionAll()   → alias of union() since Spark 3
        - unionByName() → concatenates by column NAME
        - distinct()   → removes duplicates from the result
    """
    print("\n" + "=" * 60)
    print("SET OPERATIONS — union / unionAll / unionByName / distinct")
    print("=" * 60)

    current = (spark.read.table("silver_dim_customers_type2")
                        .filter(col("is_current") == True)
                        .select("customer_id", "name", "city"))

    historical = (spark.read.table("silver_dim_customers_type2")
                           .filter(col("is_current") == False)
                           .select("customer_id", "name", "city"))

    # UNION — by position; both sides must share column order and types
    print("\nUNION — current + historical versions")
    df_union = current.union(historical)
    df_union.orderBy("customer_id").show(truncate=False)

    # UNION + DISTINCT
    print("\nUNION + DISTINCT — duplicates removed")
    df_union.distinct().orderBy("customer_id").show(truncate=False)

    # UNION BY NAME — matches columns by name, order does not matter
    print("\nUNION BY NAME — column order is irrelevant")
    reordered = historical.select("city", "name", "customer_id")
    current.unionByName(reordered).orderBy("customer_id").show(truncate=False)


# =========================================================
# TIME TRAVEL
# =========================================================
def query_time_travel(spark):
    """
    Delta Lake time travel — reads a previous version of a table.

    DESCRIBE HISTORY lists every commit; VERSION AS OF reads the table exactly
    as it was at that commit.
    """
    print("\n" + "=" * 60)
    print("DELTA TIME TRAVEL")
    print("=" * 60)

    print("\nCommit history of silver_dim_customers_type1:")
    (spark.sql("DESCRIBE HISTORY silver_dim_customers_type1")
          .select("version", "timestamp", "operation")
          .show(truncate=False))

    print("\nTable as of version 0 (empty — right after creation):")
    try:
        spark.sql("""
            SELECT customer_id, name, city, update_date
            FROM silver_dim_customers_type1 VERSION AS OF 0
            ORDER BY customer_id
        """).show(truncate=False)
    except Exception as e:
        print(f"Time travel not available: {e}")


# =========================================================
# MAIN
# =========================================================
def main():
    """Runs every demonstration query in order."""
    spark = get_spark()
    setup_unity_catalog(spark)

    # Bronze
    query_bronze(spark)

    # Silver — customers
    query_silver_dim_type2_current(spark)
    query_silver_dim_type2_history(spark)
    query_silver_point_in_time(spark, "2026-03-15")

    # Silver — products
    query_products_current(spark)
    query_products_history(spark)

    # Gold
    query_gold_sales(spark)
    query_gold_sales_by_category(spark)
    query_gold_ranking(spark)

    # Read operations
    demonstrate_join_types(spark)
    demonstrate_unions(spark)
    query_time_travel(spark)

    print("\nQueries completed.")


main()
