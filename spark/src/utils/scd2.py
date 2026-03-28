from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.window import Window
from delta.tables import DeltaTable


def apply_scd_type2(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    id_column: str,
    active_flag_column: str,
    timestamp_column: str,
    partition_columns: list,
    surrogate_key_column: str = None,
) -> None:
    """
    Apply Slowly Changing Dimension Type 2 (SCD2) logic to data using Delta Lake.

    This function handles both initial load and incremental updates for SCD2 implementation.
    It creates or updates a Delta table with effective_from, effective_to, is_active, and surrogate key columns.

    Args:
        spark (SparkSession): Active Spark session
        source_path (str): Path to the source Delta table (cleaned data)
        target_path (str): Path to the target Delta table (curated SCD2 data)
        id_column (str): Column name for the business key/identifier
        active_flag_column (str): Column name for the active flag (e.g., 'is_active')
        timestamp_column (str): Column name for the timestamp to determine version order
        partition_columns (list): List of column names to partition the target table by
        surrogate_key_column (str): Column name for the surrogate key (e.g., 'sk_customer').
                                   If None, will use 'sk_' + id_column as default.

    Returns:
        None: The function performs the SCD2 operation and saves the result to target_path

    Raises:
        Exception: If there are issues with Delta table operations or data processing
    """

    # Read the source data
    df_cleaned = spark.read.format("delta").load(source_path)

    # Determine surrogate key column name
    if surrogate_key_column is None:
        surrogate_key_column = f"sk_{id_column}"

    # Check if the target Delta table already exists
    delta_table_exists = DeltaTable.isDeltaTable(spark, target_path)
    print(f"Delta Table Exists: {delta_table_exists}")

    if not delta_table_exists:
        # Initial load - create the SCD2 table with surrogate keys
        window_spec = Window.partitionBy(id_column).orderBy(
            sf.col(timestamp_column).asc()
        )

        # Generate surrogate keys for initial load
        df_with_surrogate = (
            df_cleaned.withColumn(
                "row_num",
                sf.row_number().over(Window.orderBy(id_column, timestamp_column)),
            )
            .withColumn(surrogate_key_column, sf.col("row_num"))
            .drop("row_num")
        )

        df_scd2 = (
            df_with_surrogate.withColumn("effective_from", sf.col(timestamp_column))
            .withColumn("effective_to", sf.lead(timestamp_column).over(window_spec))
            .withColumn(
                active_flag_column,
                sf.when(sf.col("effective_to").isNull(), True).otherwise(False),
            )
        )

        # Write the initial SCD2 table with partitioning
        df_scd2.write.format("delta").partitionBy(partition_columns).mode(
            "overwrite"
        ).save(target_path)
        print(f"Initial SCD2 table with surrogate keys created at: {target_path}")

    else:
        # Incremental update - merge new data with existing SCD2 table
        curated_delta_table = DeltaTable.forPath(spark, target_path)
        df_curated = curated_delta_table.toDF()

        # Find the last loaded timestamp
        max_loaded_row = df_curated.agg({timestamp_column: "max"}).collect()[0]
        max_loaded = (
            max_loaded_row[0] if max_loaded_row[0] is not None else "1900-01-01"
        )
        print(f"Last Loaded Date: {max_loaded}")

        # Get new data since last load
        df_cleaned_new = df_cleaned.filter(sf.col(timestamp_column) > max_loaded)
        new_count = df_cleaned_new.count()
        print(f"New Data Count: {new_count}")

        if new_count == 0:
            print("No new data to process")
            return

        # Find ids that will be affected by the new data
        ids_affected = df_cleaned_new.select(id_column).distinct()
        affected_count = ids_affected.count()
        print(f"Ids Affected: {affected_count}")

        # Get current active records for affected ids
        df_curated_active = df_curated.filter(sf.col(active_flag_column) == True).join(
            ids_affected, id_column, "inner"
        )

        # Add surrogate key column with None values to new records before union
        df_cleaned_new_with_surrogate = df_cleaned_new.withColumn(
            surrogate_key_column, sf.lit(None)
        )

        # Apply SCD2 logic to the unioned dataset
        window_spec = Window.partitionBy(id_column).orderBy(
            sf.col(timestamp_column).asc()
        )

        # Union active records with new data for SCD2 processing
        df_union_cleaned_new_and_curated_active = df_curated_active.select(
            df_cleaned_new_with_surrogate.columns
        ).unionByName(df_cleaned_new_with_surrogate)

        df_scd2_base = (
            df_union_cleaned_new_and_curated_active.withColumn(
                "effective_from", sf.col(timestamp_column)
            )
            .withColumn("effective_to", sf.lead(timestamp_column).over(window_spec))
            .withColumn(
                active_flag_column,
                sf.when(sf.col("effective_to").isNull(), True).otherwise(False),
            )
        )

        # Generate surrogate keys for incremental update
        # Find the maximum existing surrogate key value
        max_surrogate_row = df_curated.agg({surrogate_key_column: "max"}).collect()[0]
        max_surrogate = max_surrogate_row[0] if max_surrogate_row[0] is not None else 0
        print(f"Max existing surrogate key: {max_surrogate}")

        # Generate new surrogate keys only for new records while preserving existing ones
        # Add a flag to identify new records vs existing active records
        df_with_source_flag = df_scd2_base.withColumn(
            "is_new_record",
            sf.when(sf.col(timestamp_column) > max_loaded, True).otherwise(False),
        )

        # Generate row numbers only for new records
        new_records_window = Window.orderBy(id_column, timestamp_column)
        df_with_new_surrogate = (
            df_with_source_flag.withColumn(
                "new_row_num",
                sf.when(
                    sf.col("is_new_record"), sf.row_number().over(new_records_window)
                ).otherwise(sf.lit(None)),
            )
            .withColumn(
                surrogate_key_column,
                sf.when(
                    sf.col("is_new_record"),
                    sf.col("new_row_num") + sf.lit(max_surrogate),
                ).otherwise(sf.col(surrogate_key_column)),
            )
            .drop("is_new_record", "new_row_num")
        )

        # Merge the SCD2 data back to the Delta table
        (
            curated_delta_table.alias("tgt")
            .merge(
                df_with_new_surrogate.alias("src"),
                f"tgt.{id_column} = src.{id_column} AND tgt.{timestamp_column} = src.{timestamp_column}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(
            f"SCD2 incremental update with surrogate keys completed at: {target_path}"
        )


# Example usage functions
def example_usage_customers_scd2(spark: SparkSession) -> None:
    """
    Example usage of apply_scd_type2 for customer data.

    This example demonstrates how to use the SCD2 function for customer dimension
    with typical e-commerce customer attributes.

    Args:
        spark (SparkSession): Active Spark session
    """
    # Example paths
    CLEANED_PATH = "/data/cleaned/customers"
    CURATED_PATH = "/data/curated/dim_customers"

    # Apply SCD2 for customer data
    apply_scd_type2(
        spark=spark,
        source_path=CLEANED_PATH,
        target_path=CURATED_PATH,
        id_column="customer_id",
        active_flag_column="is_active",
        timestamp_column="created_at",
        partition_columns=["creation_year", "creation_month"],
    )


def example_usage_products_scd2(spark: SparkSession) -> None:
    """
    Example usage of apply_scd_type2_with_custom_columns for product data.

    This example demonstrates how to use the enhanced SCD2 function for product
    dimension with custom column names for effective dates.

    Args:
        spark (SparkSession): Active Spark session
    """
    # Example paths
    CLEANED_PATH = "/data/cleaned/products"
    CURATED_PATH = "/data/curated/dim_products"

    # Apply SCD2 for product data with custom column names
    apply_scd_type2_with_custom_columns(
        spark=spark,
        source_path=CLEANED_PATH,
        target_path=CURATED_PATH,
        id_column="product_id",
        active_flag_column="is_current",
        timestamp_column="updated_at",
        partition_columns=["category_id"],
        effective_from_column="valid_from",
        effective_to_column="valid_to",
    )


def example_usage_orders_scd2(spark: SparkSession) -> None:
    """
    Example usage of apply_scd_type2 for order data.

    This example demonstrates how to use the SCD2 function for order fact table
    with order-level changes tracking.

    Args:
        spark (SparkSession): Active Spark session
    """
    # Example paths
    CLEANED_PATH = "/data/cleaned/orders"
    CURATED_PATH = "/data/curated/fact_orders_scd2"

    # Apply SCD2 for order data
    apply_scd_type2(
        spark=spark,
        source_path=CLEANED_PATH,
        target_path=CURATED_PATH,
        id_column="order_id",
        active_flag_column="is_latest",
        timestamp_column="order_date",
        partition_columns=["order_year", "order_month"],
    )


def apply_scd_type2_with_custom_columns(
    spark: SparkSession,
    source_path: str,
    target_path: str,
    id_column: str,
    active_flag_column: str,
    timestamp_column: str,
    partition_columns: list,
    effective_from_column: str = "effective_from",
    effective_to_column: str = "effective_to",
) -> None:
    """
    Apply SCD Type 2 with customizable column names for effective dates.

    This is an enhanced version that allows customization of the effective date column names.

    Args:
        spark (SparkSession): Active Spark session
        source_path (str): Path to the source Delta table (cleaned data)
        target_path (str): Path to the target Delta table (curated SCD2 data)
        id_column (str): Column name for the business key/identifier
        active_flag_column (str): Column name for the active flag (e.g., 'is_active')
        timestamp_column (str): Column name for the timestamp to determine version order
        partition_columns (list): List of column names to partition the target table by
        effective_from_column (str): Column name for effective start date (default: 'effective_from')
        effective_to_column (str): Column name for effective end date (default: 'effective_to')

    Returns:
        None: The function performs the SCD2 operation and saves the result to target_path
    """

    # Read the source data
    df_cleaned = spark.read.format("delta").load(source_path)

    # Check if the target Delta table already exists
    delta_table_exists = DeltaTable.isDeltaTable(spark, target_path)
    print(f"Delta Table Exists: {delta_table_exists}")

    if not delta_table_exists:
        # Initial load - create the SCD2 table
        window_spec = Window.partitionBy(id_column).orderBy(
            sf.col(timestamp_column).asc()
        )

        df_cleaned = (
            df_cleaned.withColumn(effective_from_column, sf.col(timestamp_column))
            .withColumn(
                effective_to_column, sf.lead(timestamp_column).over(window_spec)
            )
            .withColumn(
                active_flag_column,
                sf.when(sf.col(effective_to_column).isNull(), True).otherwise(False),
            )
        )

        # Write the initial SCD2 table with partitioning
        df_cleaned.write.format("delta").partitionBy(partition_columns).mode(
            "overwrite"
        ).save(target_path)
        print(f"Initial SCD2 table created at: {target_path}")

    else:
        # Incremental update - merge new data with existing SCD2 table
        curated_delta_table = DeltaTable.forPath(spark, target_path)
        df_curated = curated_delta_table.toDF()

        # Find the last loaded timestamp
        max_loaded_row = df_curated.agg({timestamp_column: "max"}).collect()[0]
        max_loaded = (
            max_loaded_row[0] if max_loaded_row[0] is not None else "1900-01-01"
        )
        print(f"Last Loaded Date: {max_loaded}")

        # Get new data since last load
        df_cleaned_new = df_cleaned.filter(sf.col(timestamp_column) > max_loaded)
        new_count = df_cleaned_new.count()
        print(f"New Data Count: {new_count}")

        if new_count == 0:
            print("No new data to process")
            return

        # Find customers that will be affected by the new data
        ids_affected = df_cleaned_new.select(id_column).distinct()
        affected_count = ids_affected.count()
        print(f"Customers Affected: {affected_count}")

        # Get current active records for affected customers
        df_curated_active = df_curated.filter(sf.col(active_flag_column) == True).join(
            ids_affected, id_column, "inner"
        )

        # Union active records with new data for SCD2 processing
        df_union_cleaned_new_and_curated_active = df_curated_active.select(
            df_cleaned_new.columns
        ).unionByName(df_cleaned_new)

        # Apply SCD2 logic to the unioned dataset
        window_spec = Window.partitionBy(id_column).orderBy(
            sf.col(timestamp_column).asc()
        )
        df_scd2 = (
            df_union_cleaned_new_and_curated_active.withColumn(
                effective_from_column, sf.col(timestamp_column)
            )
            .withColumn(
                effective_to_column, sf.lead(timestamp_column).over(window_spec)
            )
            .withColumn(
                active_flag_column,
                sf.when(sf.col(effective_to_column).isNull(), True).otherwise(False),
            )
        )

        # Merge the SCD2 data back to the Delta table
        merge_condition = f"tgt.{id_column} = src.{id_column} AND tgt.{timestamp_column} = src.{timestamp_column}"

        (
            curated_delta_table.alias("tgt")
            .merge(df_scd2.alias("src"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print(f"SCD2 incremental update completed at: {target_path}")
