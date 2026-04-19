#!/usr/bin/env python3
"""
Product Data Curation with SCD Type 2 Implementation

This script applies Slowly Changing Dimension Type 2 (SCD2) logic to product data
using Delta Lake. It handles both initial load and incremental updates for product
dimension tables with proper versioning and historical tracking.

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

# Import configuration and utilities
from utils.config import (
    ACCESS_KEY,
    SECRET_KEY,
    S3_ENDPOINT,
    CLEAN_PATH_PRODUCTS,
    CURATED_PATH_PRODUCTS,
)
from utils.scd2 import apply_scd_type2


def setup_spark_session(app_name: str = "curate_products") -> SparkSession:
    """
    Initialize and configure Spark session for product data curation.

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
        cleaned_path (str): Path to cleaned product data
        curated_path (str): Path to curated product data

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


def curate_products_scd2(
    cleaned_path: str, curated_path: str, spark: Optional[SparkSession] = None
) -> None:
    """
    Apply SCD Type 2 curation to product data.

    This function reads cleaned product data, applies SCD2 logic, and writes
    the curated result to the specified location with proper versioning.

    Args:
        cleaned_path (str): Path to the cleaned product Delta table
        curated_path (str): Path where the curated SCD2 product table will be saved
        spark (Optional[SparkSession]): Existing Spark session (will create new if None)

    Returns:
        None: The curated data is saved to the specified path

    Example:
        >>> curate_products_scd2(
        ...     cleaned_path="/data/cleaned/products",
        ...     curated_path="/data/curated/dim_products"
        ... )
    """

    # Setup Spark session if not provided
    if spark is None:
        spark = setup_spark_session("curate_products")

    try:
        # Validate paths
        validate_paths(cleaned_path, curated_path)

        print("=" * 60)
        print("PRODUCT DATA CURATION WITH SCD2")
        print("=" * 60)
        print(f"Source: {cleaned_path}")
        print(f"Target: {curated_path}")
        print()

        # Apply SCD2 logic using the utility function with surrogate key
        apply_scd_type2(
            spark=spark,
            source_path=cleaned_path,
            target_path=curated_path,
            id_column="product_id",
            active_flag_column="is_active",
            timestamp_column="created_at",
            partition_columns=["creation_year", "creation_month"],
            surrogate_key_column="sk_product",
        )

        print("=" * 60)
        print("PRODUCT CURATION COMPLETED SUCCESSFULLY")
        print("=" * 60)

        # Display final table information
        if DeltaTable.isDeltaTable(spark, curated_path):
            curated_table = DeltaTable.forPath(spark, curated_path)
            df_curated = curated_table.toDF()

            print(f"Final table schema:")
            df_curated.printSchema()

            total_records = df_curated.count()
            active_records = df_curated.filter(sf.col("is_active") == True).count()

            print(f"Total records: {total_records:,}")
            print(f"Active records: {active_records:,}")
            print(f"Active percentage: {(active_records/total_records)*100:.2f}%")

            # Show sample data
            print("\nSample of curated data:")
            df_curated.orderBy("product_id", "effective_from").limit(10).show(
                truncate=False
            )

    except Exception as e:
        print(f"ERROR during product curation: {str(e)}")
        raise
    finally:
        # Stop Spark session if we created it
        if spark and spark.sparkContext._active_spark_context:
            spark.stop()


def main():
    """
    Main entry point for the product curation script.

    This function orchestrates the curation process using predefined paths.
    """
    try:
        print("Starting product data curation...")
        print(f"Using cleaned data path: {CLEAN_PATH_PRODUCTS}")
        print(f"Using curated data path: {CURATED_PATH_PRODUCTS}")
        print()

        curate_products_scd2(
            cleaned_path=CLEAN_PATH_PRODUCTS, curated_path=CURATED_PATH_PRODUCTS
        )
        print("Product curation completed successfully!")

    except Exception as e:
        print(f"Product curation failed: {e}")
        raise


if __name__ == "__main__":
    main()
