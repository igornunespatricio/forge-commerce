#!/usr/bin/env python3
"""
Payment Fact Table Curation

This script curates payment data for the fact table by:
1. Flattening the payment_metadata struct into individual columns
2. Getting the latest payment record per payment_id based on created_at
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
PREFIX = "payments"
RAW_BUCKET = "raw"
CLEANED_BUCKET = "cleaned"
CURATED_BUCKET = "curated"
RAW_PATH = f"s3a://{RAW_BUCKET}/{PREFIX}/"
CLEANED_PATH = f"s3a://{CLEANED_BUCKET}/{PREFIX}/"
CURATED_PATH = f"s3a://{CURATED_BUCKET}/{PREFIX}/"


def setup_spark_session(app_name: str = "curate_payments") -> SparkSession:
    """
    Initialize and configure Spark session for payment fact table curation.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Delta Lake configurations
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")

    return spark


def validate_paths(cleaned_path: str, curated_path: str) -> None:
    """
    Validate that the provided paths are accessible and appropriate.

    Args:
        cleaned_path (str): Path to cleaned payment data
        curated_path (str): Path to curated payment fact table data

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


def get_latest_payment_records(df_payments):
    """
    Get the latest payment record per payment_id based on created_at timestamp.

    Args:
        df_payments (DataFrame): Cleaned payments DataFrame

    Returns:
        DataFrame: Payments with only the latest record per payment_id
    """
    # Window specification to get the latest record per payment_id
    window_spec = Window.partitionBy("payment_id").orderBy(sf.col("created_at").desc())

    # Add row number to identify the latest record
    df_with_row_num = df_payments.withColumn(
        "row_num", sf.row_number().over(window_spec)
    )

    # Filter to keep only the latest record (row_num = 1)
    df_latest_payments = df_with_row_num.filter(sf.col("row_num") == 1).drop("row_num")

    return df_latest_payments


def flatten_payment_metadata(df_payments):
    """
    Flatten the payment_metadata struct into individual columns.

    Args:
        df_payments (DataFrame): Payments DataFrame with payment_metadata struct

    Returns:
        DataFrame: Payments with flattened metadata columns
    """
    df_flattened = df_payments.select(
        # Primary keys and foreign keys
        "payment_id",
        "order_id",
        "customer_id",
        # Payment amounts and fees
        "order_amount",
        "net_amount",
        "transaction_fee",
        "transaction_fee_rate",
        "chargeback_amount",
        # Payment details
        "payment_method",
        "payment_gateway",
        "payment_status",
        "currency_code",
        "payment_reference",
        "payment_uuid",
        # Payment timing
        "payment_date",
        "payment_time",
        "created_at",
        "updated_at",
        "payment_timestamp",
        # Chargeback information
        "chargeback_date",
        "chargeback_reason",
        # Risk and fraud indicators
        "fraud_score",
        "fraud_risk_level",
        # Customer segment
        "customer_segment",
        # Flatten payment_metadata struct
        sf.col("payment_metadata.browser").alias("browser"),
        sf.col("payment_metadata.device_type").alias("device_type"),
        sf.col("payment_metadata.ip_address").alias("ip_address"),
        sf.col("payment_metadata.user_agent").alias("user_agent"),
        # Boolean flags for analytics
        "is_successful_payment",
        "is_failed_payment",
        "is_pending_payment",
        "has_chargeback",
        # Data quality flags
        "missing_payment_date_flag",
        "missing_chargeback_date_flag",
        "long_chargeback_time_flag",
        "negative_net_amount_flag",
        "status_inconsistency_flag",
        # Partition columns
        "payment_year",
        "payment_month",
        # Computed columns for analytics
        sf.hour(sf.col("payment_timestamp")).alias("payment_hour"),
        sf.dayofweek(sf.col("payment_timestamp")).alias("payment_day_of_week"),
    )

    return df_flattened


def join_with_customer_dimension(df_payments, spark, customer_path):
    """
    Join payments with customer dimension using temporal logic to get correct surrogate key.

    This function ensures we get the correct customer surrogate key based on the payment's
    created_at timestamp, considering the customer's effective date ranges.

    Args:
        df_payments (DataFrame): Payments DataFrame with customer_id
        spark (SparkSession): Active Spark session
        customer_path (str): Path to curated customer dimension table

    Returns:
        DataFrame: Payments joined with customer surrogate key
    """
    # Read customer dimension table
    df_customers = spark.read.format("delta").load(customer_path)

    print(f"Customer dimension records: {df_customers.count()}")
    print(f"Payment records before join: {df_payments.count()}")

    # Join payments with customers using temporal logic
    # We need to find the customer record that was active when the payment was created
    df_joined = df_payments.join(
        df_customers,
        (df_payments.customer_id == df_customers.customer_id)
        & (df_payments.created_at >= df_customers.effective_from)
        & (
            (df_customers.effective_to.isNull())
            | (df_payments.created_at < df_customers.effective_to)
        ),
        "inner",
    )

    # Select required columns, replacing customer_id with sk_customer
    df_result = df_joined.select(
        df_payments["*"],  # All payment columns
        df_customers["sk_customer"],  # Customer surrogate key
    ).drop(
        df_customers["customer_id"]
    )  # Remove the business key to avoid duplication

    print(f"Payment records after customer join: {df_result.count()}")

    return df_result


def curate_payment_fact(
    cleaned_path: str, curated_path: str, spark: Optional[SparkSession] = None
) -> None:
    """
    Curate payment data for fact table.

    This function reads cleaned payment data, flattens payment_metadata struct, gets latest records,
    joins with customer dimension using temporal logic, and writes the curated result.

    Args:
        cleaned_path (str): Path to the cleaned payment Delta table
        curated_path (str): Path where the curated payment fact table will be saved
        spark (Optional[SparkSession]): Existing Spark session (will create new if None)

    Returns:
        None: The curated data is saved to the specified path

    Example:
        >>> curate_payment_fact(
        ...     cleaned_path="/data/cleaned/payments",
        ...     curated_path="/data/curated/fact_payments"
        ... )
    """

    # Setup Spark session if not provided
    if spark is None:
        spark = setup_spark_session("curate_payments")

    try:
        # Validate paths
        validate_paths(cleaned_path, curated_path)

        print("=" * 60)
        print("PAYMENT FACT TABLE CURATION")
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
            print(f"Last loaded payment date: {max_loaded}")

        # Read cleaned payments data
        df_cleaned_payments = spark.read.format("delta").load(cleaned_path)

        # OPTIMIZATION: Filter payments BEFORE expensive operations
        if max_loaded != "1900-01-01":
            df_cleaned_payments = df_cleaned_payments.filter(
                sf.col("created_at") > max_loaded
            )
            print(f"Filtered payments to process: {df_cleaned_payments.count()}")

        print(f"Cleaned payments records: {df_cleaned_payments.count()}")

        # Get latest payment records per payment_id
        df_latest_payments = get_latest_payment_records(df_cleaned_payments)
        print(f"Latest payment records: {df_latest_payments.count()}")

        # Flatten payment_metadata struct into individual columns
        df_flattened = flatten_payment_metadata(df_latest_payments)
        print(f"Flattened payment records: {df_flattened.count()}")

        # Define customer dimension path
        customer_dimension_path = f"s3a://{CURATED_BUCKET}/customers/"

        # Join with customer dimension using temporal logic
        df_with_customer_sk = join_with_customer_dimension(
            df_flattened, spark, customer_dimension_path
        )

        # Write to curated location with partitioning
        if not delta_table_exists:
            # Initial load
            df_with_customer_sk.write.format("delta").partitionBy(
                "payment_year", "payment_month"
            ).mode("overwrite").save(curated_path)
            print(f"Initial payment fact table created at: {curated_path}")
        else:
            # Incremental update
            curated_table = DeltaTable.forPath(spark, curated_path)

            df_new_payments = df_with_customer_sk.filter(
                sf.col("created_at") > max_loaded
            )
            new_count = df_new_payments.count()
            print(f"New payment records to insert: {new_count}")

            if new_count > 0:
                curated_table.alias("tgt").merge(
                    df_new_payments.alias("src"),
                    "tgt.payment_id = src.payment_id AND tgt.created_at = src.created_at",
                ).whenNotMatchedInsertAll().execute()
                print(f"Payment fact table incremental update completed")
            else:
                print("No new payments to process")

        print("=" * 60)
        print("PAYMENT FACT TABLE CURATION COMPLETED SUCCESSFULLY")
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
            print("\nSample of curated payment fact data:")
            df_curated.orderBy("payment_id").limit(5).show(truncate=False)

            # Verify customer surrogate key integration
            customer_sk_count = df_curated.filter(
                sf.col("sk_customer").isNotNull()
            ).count()
            print(f"Payments with valid customer surrogate keys: {customer_sk_count:,}")

            # Show data distribution by partition
            print("\nData distribution by partition:")
            df_curated.groupBy("payment_year", "payment_month").count().orderBy(
                "payment_year", "payment_month"
            ).show()

            # Show payment status distribution
            print("\nPayment status distribution:")
            df_curated.groupBy("payment_status").count().orderBy(
                "count", ascending=False
            ).show()

            # Show successful vs failed payments
            print("\nSuccessful vs Failed payments:")
            df_curated.groupBy(
                "is_successful_payment", "is_failed_payment"
            ).count().show()

    except Exception as e:
        print(f"ERROR during payment fact table curation: {str(e)}")
        raise
    finally:
        # Stop Spark session if we created it
        if spark and spark.sparkContext._active_spark_context:
            spark.stop()


def main():
    """
    Main entry point for the payment fact table curation script.

    This function orchestrates the curation process using predefined paths.
    """
    try:
        print("Starting payment fact table curation...")
        print(f"Using cleaned data path: {CLEANED_PATH}")
        print(f"Using curated data path: {CURATED_PATH}")
        print()

        curate_payment_fact(cleaned_path=CLEANED_PATH, curated_path=CURATED_PATH)
        print("Payment fact table curation completed successfully!")

    except Exception as e:
        print(f"Payment fact table curation failed: {e}")
        raise


if __name__ == "__main__":
    main()
