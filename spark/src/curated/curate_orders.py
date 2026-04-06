#!/usr/bin/env python3
"""
Order Fact Table Curation

This script curates order data for the fact table by:
1. Removing the order_items array (handled separately in order_items fact table)
2. Getting the latest order record per order_id based on created_at
3. Joining with customer dimension using temporal logic to get correct surrogate key
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

# Environment configuration
ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "forge-commerce-user")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "forge-commerce-pass")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
PREFIX = "orders"
RAW_BUCKET = "raw"
CLEANED_BUCKET = "cleaned"
CURATED_BUCKET = "curated"
RAW_PATH = f"s3a://{RAW_BUCKET}/{PREFIX}/"
CLEANED_PATH = f"s3a://{CLEANED_BUCKET}/{PREFIX}/"
CURATED_PATH = f"s3a://{CURATED_BUCKET}/{PREFIX}/"


def setup_spark_session(app_name: str = "curate_orders") -> SparkSession:
    """
    Initialize and configure Spark session for order fact table curation.

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
        curated_path (str): Path to curated order fact table data

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


def join_with_customer_dimension(df_orders, spark, customer_path):
    """
    Join orders with customer dimension using temporal logic to get correct surrogate key.

    This function ensures we get the correct customer surrogate key based on the order's
    created_at timestamp, considering the customer's effective date ranges.

    Args:
        df_orders (DataFrame): Orders DataFrame with customer_id
        spark (SparkSession): Active Spark session
        customer_path (str): Path to curated customer dimension table

    Returns:
        DataFrame: Orders joined with customer surrogate key
    """
    # Read customer dimension table
    df_customers = spark.read.format("delta").load(customer_path)

    print(f"Customer dimension records: {df_customers.count()}")
    print(f"Order records before join: {df_orders.count()}")

    # Join orders with customers using temporal logic
    # We need to find the customer record that was active when the order was created
    df_joined = df_orders.join(
        df_customers,
        (df_orders.customer_id == df_customers.customer_id)
        & (df_orders.created_at >= df_customers.effective_from)
        & (
            (df_customers.effective_to.isNull())
            | (df_orders.created_at < df_customers.effective_to)
        ),
        "inner",
    )

    # Select required columns, replacing customer_id with sk_customer
    df_result = df_joined.select(
        df_orders["*"],  # All order columns
        df_customers["sk_customer"],  # Customer surrogate key
    ).drop(
        df_customers["customer_id"]
    )  # Remove the business key

    print(f"Order records after customer join: {df_result.count()}")

    return df_result


def curate_order_fact(
    cleaned_path: str, curated_path: str, spark: Optional[SparkSession] = None
) -> None:
    """
    Curate order data for fact table.

    This function reads cleaned order data, removes order_items array, gets latest records,
    joins with customer dimension using temporal logic, and writes the curated result.

    Args:
        cleaned_path (str): Path to the cleaned order Delta table
        curated_path (str): Path where the curated order fact table will be saved
        spark (Optional[SparkSession]): Existing Spark session (will create new if None)

    Returns:
        None: The curated data is saved to the specified path

    Example:
        >>> curate_order_fact(
        ...     cleaned_path="/data/cleaned/orders",
        ...     curated_path="/data/curated/fact_orders"
        ... )
    """

    # Setup Spark session if not provided
    if spark is None:
        spark = setup_spark_session("curate_orders")

    try:
        # Validate paths
        validate_paths(cleaned_path, curated_path)

        print("=" * 60)
        print("ORDER FACT TABLE CURATION")
        print("=" * 60)
        print(f"Source: {cleaned_path}")
        print(f"Target: {curated_path}")
        print()

        # Read cleaned orders data
        df_cleaned_orders = spark.read.format("delta").load(cleaned_path)
        print(f"Cleaned orders records: {df_cleaned_orders.count()}")

        # Get latest order records per order_id
        df_latest_orders = get_latest_order_records(df_cleaned_orders)
        print(f"Latest order records: {df_latest_orders.count()}")

        # Remove order_items array (handled in separate fact table)
        columns_to_keep = [
            col for col in df_latest_orders.columns if col != "order_items"
        ]
        df_orders_no_items = df_latest_orders.select(*columns_to_keep)
        print(f"Order records after removing order_items: {df_orders_no_items.count()}")

        # Define customer dimension path
        customer_dimension_path = f"s3a://{CURATED_BUCKET}/customers/"

        # Join with customer dimension using temporal logic
        df_with_customer_sk = join_with_customer_dimension(
            df_orders_no_items, spark, customer_dimension_path
        )

        # Write to curated location with partitioning
        delta_table_exists = DeltaTable.isDeltaTable(spark, curated_path)

        if not delta_table_exists:
            # Initial load
            df_with_customer_sk.write.format("delta").partitionBy(
                "order_year", "order_month"
            ).mode("overwrite").save(curated_path)
            print(f"Initial order fact table created at: {curated_path}")
        else:
            # Incremental update
            curated_table = DeltaTable.forPath(spark, curated_path)

            # Get new orders since last load
            max_loaded_row = (
                curated_table.toDF().agg({"created_at": "max"}).collect()[0]
            )
            max_loaded = (
                max_loaded_row[0] if max_loaded_row[0] is not None else "1900-01-01"
            )
            print(f"Last loaded order date: {max_loaded}")

            df_new_orders = df_with_customer_sk.filter(
                sf.col("created_at") > max_loaded
            )
            new_count = df_new_orders.count()
            print(f"New order records to insert: {new_count}")

            if new_count > 0:
                curated_table.alias("tgt").merge(
                    df_new_orders.alias("src"),
                    "tgt.order_id = src.order_id AND tgt.created_at = src.created_at",
                ).whenNotMatchedInsertAll().execute()
                print(f"Order fact table incremental update completed")
            else:
                print("No new orders to process")

        print("=" * 60)
        print("ORDER FACT TABLE CURATION COMPLETED SUCCESSFULLY")
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
            print("\nSample of curated order fact data:")
            df_curated.orderBy("order_id").limit(5).show(truncate=False)

            # Verify customer surrogate key integration
            customer_sk_count = df_curated.filter(
                sf.col("sk_customer").isNotNull()
            ).count()
            print(f"Orders with valid customer surrogate keys: {customer_sk_count:,}")

    except Exception as e:
        print(f"ERROR during order fact table curation: {str(e)}")
        raise
    finally:
        # Stop Spark session if we created it
        if spark and spark.sparkContext._active_spark_context:
            spark.stop()


def main():
    """
    Main entry point for the order fact table curation script.

    This function orchestrates the curation process using predefined paths.
    """
    try:
        print("Starting order fact table curation...")
        print(f"Using cleaned data path: {CLEANED_PATH}")
        print(f"Using curated data path: {CURATED_PATH}")
        print()

        curate_order_fact(cleaned_path=CLEANED_PATH, curated_path=CURATED_PATH)
        print("Order fact table curation completed successfully!")

    except Exception as e:
        print(f"Order fact table curation failed: {e}")
        raise


if __name__ == "__main__":
    main()
