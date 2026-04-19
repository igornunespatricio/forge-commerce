#!/usr/bin/env python3
"""
Order Items Fact Table Curation

This script curates order items data for the fact table by:
1. Exploding the order_items array from cleaned orders
2. Getting the latest order record per order_id based on created_at
3. Joining with product dimension using temporal logic to get correct surrogate key
4. Creating a clean fact table with proper surrogate key relationships

Author: Generated for forge-commerce project
"""

from typing import Optional

from pyspark.sql import SparkSession
import json
import os
import sys
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# Get the absolute path to the spark directory
spark_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, spark_dir)

# Import configuration
from utils.config import (
    ACCESS_KEY,
    SECRET_KEY,
    S3_ENDPOINT,
    CLEAN_PATH_ORDERS,
    CURATED_PATH_ORDER_ITEMS,
    CURATED_PATH_PRODUCTS,
)


def setup_spark_session(app_name: str = "curate_order_items") -> SparkSession:
    """
    Initialize and configure Spark session for order items fact table curation.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        # .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        # .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        # .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        # .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # # Delta Lake configurations
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config(
        #     "spark.sql.catalog.spark_catalog",
        #     "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # )
        .getOrCreate()
    )

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark


def validate_paths(cleaned_path: str, curated_path: str) -> None:
    """
    Validate that the provided paths are accessible and appropriate.

    Args:
        cleaned_path (str): Path to cleaned order data
        curated_path (str): Path to curated order items fact table data

    Raises:
        ValueError: If paths are invalid or inaccessible
    """
    if not cleaned_path or not curated_path:
        raise ValueError("Both cleaned_path and curated_path must be provided")

    if cleaned_path == curated_path:
        raise ValueError("Cleaned and curated paths must be different")

    print(f"Validating paths:")
    print(f"  Cleaned data path: {cleaned_path}")
    print(f"  Curated data path: {curated_path}")


def get_latest_order_records(df_orders):
    """
    Get the latest order record per order_id based on created_at timestamp.

    Args:
        df_orders (DataFrame): Cleaned orders DataFrame

    Returns:
        DataFrame: Orders with only the latest record per order_id
    """
    # Window specification to get the latest record per order_id
    window_spec = Window.partitionBy("order_id").orderBy(sf.col("created_at").desc())

    # Add row number to identify the latest record
    df_with_row_num = df_orders.withColumn("row_num", sf.row_number().over(window_spec))

    # Filter to keep only the latest record (row_num = 1)
    df_latest_orders = df_with_row_num.filter(sf.col("row_num") == 1).drop("row_num")

    return df_latest_orders


def explode_order_items(df_orders):
    """
    Explode the order_items array into separate rows for the fact table.

    Args:
        df_orders (DataFrame): Orders DataFrame with order_items array

    Returns:
        DataFrame: Orders with exploded order_items (one row per item)
    """
    # Explode the order_items array
    df_exploded = df_orders.select(
        "order_id",
        "customer_id",
        "created_at",
        "order_date",
        sf.explode("order_items").alias("item"),
    ).select(
        "order_id",
        "customer_id",
        "created_at",
        "order_date",
        sf.col("item.product_id").alias("product_id"),
        sf.col("item.product_name").alias("product_name"),
        sf.col("item.category").alias("category"),
        sf.col("item.subcategory").alias("subcategory"),
        sf.col("item.brand").alias("brand"),
        sf.col("item.unit_price").alias("unit_price"),
        sf.col("item.quantity").alias("quantity"),
        sf.col("item.discount_percentage").alias("discount_percentage"),
        sf.col("item.line_total").alias("line_total"),
    )

    return df_exploded


def join_with_product_dimension(df_order_items, spark, product_path):
    """
    Join order items with product dimension using temporal logic to get correct surrogate key.

    This function ensures we get the correct product surrogate key based on the order's
    created_at timestamp, considering the product's effective date ranges.

    Args:
        df_order_items (DataFrame): Order items DataFrame
        spark (SparkSession): Active Spark session
        product_path (str): Path to curated product dimension table

    Returns:
        DataFrame: Order items joined with product surrogate key
    """
    # Read product dimension table
    df_products = spark.read.format("delta").load(product_path)

    print(f"Product dimension records: {df_products.count()}")
    print(f"Order items records before join: {df_order_items.count()}")

    # Join order items with products using temporal logic
    # We need to find the product record that was active when the order was created
    df_joined = df_order_items.join(
        df_products,
        (df_order_items.product_id == df_products.product_id)
        & (df_order_items.created_at >= df_products.effective_from)
        & (
            (df_products.effective_to.isNull())
            | (df_order_items.created_at < df_products.effective_to)
        ),
        "inner",
    )

    # Select required columns, replacing product_id with sk_product
    df_result = df_joined.select(
        df_order_items["order_id"],
        df_order_items["customer_id"],
        df_order_items["product_id"],
        df_products["sk_product"],
        df_order_items["product_name"],
        df_order_items["category"],
        df_order_items["subcategory"],
        df_order_items["brand"],
        df_order_items["unit_price"],
        df_order_items["quantity"],
        df_order_items["discount_percentage"],
        df_order_items["line_total"],
        df_order_items["created_at"],
        df_order_items["order_date"],
        # Extract year and month for partitioning
        sf.year(df_order_items["order_date"]).alias("order_year"),
        sf.month(df_order_items["order_date"]).alias("order_month"),
    )

    print(f"Order items records after product join: {df_result.count()}")

    return df_result


def curate_order_items_fact(
    cleaned_path: str, curated_path: str, spark: Optional[SparkSession] = None
) -> None:
    """
    Curate order items data for fact table.

    This function reads cleaned order data, explodes order_items array, gets latest records,
    joins with product dimension using temporal logic, and writes the curated result.

    Args:
        cleaned_path (str): Path to the cleaned order Delta table
        curated_path (str): Path where the curated order items fact table will be saved
        spark (Optional[SparkSession]): Existing Spark session (will create new if None)

    Returns:
        None: The curated data is saved to the specified path

    Example:
        >>> curate_order_items_fact(
        ...     cleaned_path="/data/cleaned/orders",
        ...     curated_path="/data/curated/fact_order_items"
        ... )
    """

    # Setup Spark session if not provided
    if spark is None:
        spark = setup_spark_session("curate_order_items")

    try:
        # Validate paths
        validate_paths(cleaned_path, curated_path)

        print("=" * 60)
        print("ORDER ITEMS FACT TABLE CURATION")
        print("=" * 60)
        print(f"Source: {cleaned_path}")
        print(f"Target: {curated_path}")
        print()

        # OPTIMIZATION: Check max_loaded BEFORE expensive operations
        delta_table_exists = DeltaTable.isDeltaTable(spark, curated_path)
        max_loaded = "1900-01-01"  # Default for initial load

        if delta_table_exists:
            curated_table = DeltaTable.forPath(spark, curated_path)
            max_loaded_row = (
                curated_table.toDF().agg({"created_at": "max"}).collect()[0]
            )
            max_loaded = (
                max_loaded_row[0] if max_loaded_row[0] is not None else "1900-01-01"
            )
            print(f"Last loaded order date: {max_loaded}")

        # Read cleaned orders data
        df_cleaned_orders = spark.read.format("delta").load(cleaned_path)

        # OPTIMIZATION: Filter orders BEFORE expensive operations
        if max_loaded != "1900-01-01":
            df_cleaned_orders = df_cleaned_orders.filter(
                sf.col("created_at") > max_loaded
            )
            print(f"Filtered orders to process: {df_cleaned_orders.count()}")

        print(f"Cleaned orders records: {df_cleaned_orders.count()}")

        # Get latest order records per order_id
        df_latest_orders = get_latest_order_records(df_cleaned_orders)
        print(f"Latest order records: {df_latest_orders.count()}")

        # Explode order_items array into separate rows
        df_exploded_items = explode_order_items(df_latest_orders)
        print(f"Exploded order items records: {df_exploded_items.count()}")

        # Define product dimension path
        product_dimension_path = CURATED_PATH_PRODUCTS

        # Join with product dimension using temporal logic
        df_with_product_sk = join_with_product_dimension(
            df_exploded_items, spark, product_dimension_path
        )

        # Write to curated location with partitioning
        if not delta_table_exists:
            # Initial load
            df_with_product_sk.write.format("delta").partitionBy(
                "order_year", "order_month"
            ).mode("overwrite").save(curated_path)
            print(f"Initial order items fact table created at: {curated_path}")
        else:
            # Incremental update
            curated_table = DeltaTable.forPath(spark, curated_path)

            df_new_items = df_with_product_sk.filter(sf.col("created_at") > max_loaded)
            new_count = df_new_items.count()
            print(f"New order item records to insert: {new_count}")

            if new_count > 0:
                curated_table.alias("tgt").merge(
                    df_new_items.alias("src"),
                    "tgt.order_id = src.order_id AND tgt.product_id = src.product_id AND tgt.created_at = src.created_at",
                ).whenNotMatchedInsertAll().execute()
                print(f"Order items fact table incremental update completed")
            else:
                print("No new order items to process")

        print("=" * 60)
        print("ORDER ITEMS FACT TABLE CURATION COMPLETED SUCCESSFULLY")
        print("=" * 60)

        # Display final table information
        if DeltaTable.isDeltaTable(spark, curated_path):
            curated_table = DeltaTable.forPath(spark, curated_path)
            df_curated = curated_table.toDF()

            print(f"Final table schema:")
            df_curated.printSchema()

            total_records = df_curated.count()
            print(f"Total fact table records: {total_records:,}")

            # Show sample data
            print("\nSample of curated order items fact data:")
            df_curated.orderBy("order_id", "product_id").limit(5).show(truncate=False)

            # Verify product surrogate key integration
            product_sk_count = df_curated.filter(
                sf.col("sk_product").isNotNull()
            ).count()
            print(
                f"Order items with valid product surrogate keys: {product_sk_count:,}"
            )

            # Show data distribution by partition
            print("\nData distribution by partition:")
            df_curated.groupBy("order_year", "order_month").count().orderBy(
                "order_year", "order_month"
            ).show()

    except Exception as e:
        print(f"ERROR during order items fact table curation: {str(e)}")
        raise
    finally:
        # Stop Spark session if we created it
        if spark and spark.sparkContext._active_spark_context:
            spark.stop()


def main():
    """
    Main entry point for the order items fact table curation script.

    This function orchestrates the curation process using predefined paths.
    """
    try:
        print("Starting order items fact table curation...")
        print(f"Using cleaned data path: {CLEAN_PATH_ORDERS}")
        print(f"Using curated data path: {CURATED_PATH_ORDER_ITEMS}")
        print()

        curate_order_items_fact(
            cleaned_path=CLEAN_PATH_ORDERS, curated_path=CURATED_PATH_ORDER_ITEMS
        )
        print("Order items fact table curation completed successfully!")

    except Exception as e:
        print(f"Order items fact table curation failed: {e}")
        raise


if __name__ == "__main__":
    main()
