from delta import DeltaTable
from pyspark.sql import SparkSession
import pyspark.sql.types as st
import os
from pyspark.sql import functions as sf

ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "forge-commerce-user")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "forge-commerce-pass")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
PREFIX = "orders"
ORIGIN_BUCKET = "raw"
DESTINATION_BUCKET = "cleaned"
ORIGIN_PATH = f"s3a://{ORIGIN_BUCKET}/{PREFIX}/"
DESTINATION_PATH = f"s3a://{DESTINATION_BUCKET}/{PREFIX}/"


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("clean_orders")
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

    try:
        # Read data from MinIO
        df = spark.read.json(ORIGIN_PATH)

        # Drop duplicates based on order id and created at
        df_deduplicated = df.dropDuplicates()

        df_clean = (
            df_deduplicated
            # ----------------------------
            # Standardize / normalize text
            # ----------------------------
            .withColumn("customer_segment", sf.lower(sf.trim("customer_segment")))
            .withColumn("order_status", sf.lower(sf.trim("order_status")))
            .withColumn("payment_status", sf.lower(sf.trim("payment_status")))
            .withColumn("payment_method", sf.lower(sf.trim("payment_method")))
            .withColumn("shipping_method", sf.lower(sf.trim("shipping_method")))
            .withColumn("delivery_city", sf.initcap(sf.trim("delivery_city")))
            .withColumn("delivery_country", sf.initcap(sf.trim("delivery_country")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("order_date", sf.to_date("order_date"))
            .withColumn("fulfillment_date", sf.to_date("fulfillment_date"))
            .withColumn("delivery_date", sf.to_date("delivery_date"))
            .withColumn("created_at", sf.to_timestamp("created_at"))
            .withColumn("updated_at", sf.to_timestamp("updated_at"))
            # ----------------------------
            # Convert to integer/decimal
            # ----------------------------
            .withColumn("order_id", sf.col("order_id").cast("integer"))
            .withColumn("customer_id", sf.col("customer_id").cast("integer"))
            .withColumn(
                "discount_amount", sf.col("discount_amount").cast(st.DecimalType(10, 2))
            )
            .withColumn(
                "discount_percentage",
                sf.col("discount_percentage").cast(st.DecimalType(10, 2)),
            )
            .withColumn(
                "shipping_cost", sf.col("shipping_cost").cast(st.DecimalType(10, 2))
            )
            .withColumn("subtotal", sf.col("subtotal").cast(st.DecimalType(10, 2)))
            .withColumn("tax_amount", sf.col("tax_amount").cast(st.DecimalType(10, 2)))
            .withColumn("tax_rate", sf.col("tax_rate").cast(st.DecimalType(10, 2)))
            .withColumn(
                "total_amount", sf.col("total_amount").cast(st.DecimalType(10, 2))
            )
            # ----------------------------
            # Convert nested columns
            # ----------------------------
            .withColumn(
                "order_items",
                sf.transform(
                    "order_items",
                    lambda x: x.withField(
                        "discount_percentage",
                        x["discount_percentage"].cast(st.DecimalType(10, 2)),
                    )
                    .withField(
                        "line_total",
                        x["line_total"].cast(st.DecimalType(10, 2)),
                    )
                    .withField(
                        "unit_price",
                        x["unit_price"].cast(st.DecimalType(10, 2)),
                    )
                    .withField(
                        "quantity",
                        x["quantity"].cast("integer"),
                    )
                    .withField(
                        "product_id",
                        x["product_id"].cast("integer"),
                    ),
                ),
            )
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year and month for partitioning
            .withColumn("order_year", sf.year("order_date"))
            .withColumn("order_month", sf.month("order_date"))
            # Order age in days
            .withColumn(
                "order_age_days", sf.datediff(sf.current_date(), sf.col("order_date"))
            )
            # Fulfillment time in days
            .withColumn(
                "fulfillment_time_days",
                sf.when(
                    sf.col("fulfillment_date").isNotNull()
                    & sf.col("order_date").isNotNull(),
                    sf.datediff(sf.col("fulfillment_date"), sf.col("order_date")),
                ).otherwise(None),
            )
            # Delivery time in days
            .withColumn(
                "delivery_time_days",
                sf.when(
                    sf.col("delivery_date").isNotNull()
                    & sf.col("fulfillment_date").isNotNull(),
                    sf.datediff(sf.col("delivery_date"), sf.col("fulfillment_date")),
                ).otherwise(None),
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Validate order amounts
            .withColumn(
                "total_amount",
                sf.when(sf.col("total_amount") < 0, None).otherwise(
                    sf.col("total_amount")
                ),
            )
            .withColumn(
                "subtotal",
                sf.when(sf.col("subtotal") < 0, None).otherwise(sf.col("subtotal")),
            )
            .withColumn(
                "shipping_cost",
                sf.when(sf.col("shipping_cost") < 0, 0).otherwise(
                    sf.col("shipping_cost")
                ),
            )
            .withColumn(
                "discount_amount",
                sf.when(sf.col("discount_amount") < 0, 0).otherwise(
                    sf.col("discount_amount")
                ),
            )
            .withColumn(
                "tax_amount",
                sf.when(sf.col("tax_amount") < 0, 0).otherwise(sf.col("tax_amount")),
            )
            # Validate percentages
            .withColumn(
                "discount_percentage",
                sf.when(
                    (sf.col("discount_percentage") < 0)
                    | (sf.col("discount_percentage") > 1),
                    None,
                ).otherwise(sf.col("discount_percentage")),
            )
            .withColumn(
                "tax_rate",
                sf.when(
                    (sf.col("tax_rate") < 0) | (sf.col("tax_rate") > 1), None
                ).otherwise(sf.col("tax_rate")),
            )
            # ----------------------------
            # Data quality flags
            # ----------------------------
            # Flag orders with very high discounts
            .withColumn(
                "high_discount_flag",
                sf.when(sf.col("discount_percentage") > 0.5, True).otherwise(False),
            )
            # Flag orders with missing fulfillment dates
            .withColumn(
                "missing_fulfillment_flag",
                sf.when(sf.col("fulfillment_date").isNull(), True).otherwise(False),
            )
            # Flag orders with missing delivery dates
            .withColumn(
                "missing_delivery_flag",
                sf.when(sf.col("delivery_date").isNull(), True).otherwise(False),
            )
            # Flag orders with long fulfillment times
            .withColumn(
                "long_fulfillment_flag",
                sf.when(sf.col("fulfillment_time_days") > 14, True).otherwise(False),
            )
            # Flag orders with long delivery times
            .withColumn(
                "long_delivery_flag",
                sf.when(sf.col("delivery_time_days") > 30, True).otherwise(False),
            )
            # Flag cancelled orders with shipping costs
            .withColumn(
                "cancelled_with_shipping_flag",
                sf.when(
                    (sf.col("order_status") == "cancelled")
                    & (sf.col("shipping_cost") > 0),
                    True,
                ).otherwise(False),
            )
            # Flag orders with invalid status combinations
            .withColumn(
                "status_inconsistency_flag",
                sf.when(
                    (
                        (sf.col("order_status") == "completed")
                        & (sf.col("payment_status") != "paid")
                    )
                    | (
                        (sf.col("order_status") == "cancelled")
                        & (sf.col("payment_status") == "paid")
                    ),
                    True,
                ).otherwise(False),
            )
        )

        # Write to cleaned bucket with partitioning
        # df_clean.write.format("delta").partitionBy("order_year", "order_month").mode(
        #     "overwrite"
        # ).save(DESTINATION_PATH)

        delta_table_exists = DeltaTable.isDeltaTable(spark, DESTINATION_PATH)
        if not delta_table_exists:
            df_clean.write.format("delta").partitionBy(
                "order_year", "order_month"
            ).mode("overwrite").save(DESTINATION_PATH)
        else:
            cleaned_table = DeltaTable.forPath(spark, DESTINATION_PATH)
            (
                cleaned_table.alias("tgt")
                .merge(
                    df_clean.alias("src"),
                    "tgt.order_id = src.order_id AND tgt.created_at = src.created_at",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        print("Data processing completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
