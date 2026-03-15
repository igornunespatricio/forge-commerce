from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, current_date
import json
import os
from pyspark.sql import functions as F

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
        SparkSession.builder.appName("ReadOrdersFromRaw")
        .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    try:
        # Read data from MinIO
        df = spark.read.json(ORIGIN_PATH)

        df_clean = (
            df
            # ----------------------------
            # Standardize / normalize text
            # ----------------------------
            .withColumn("customer_segment", F.lower(F.trim("customer_segment")))
            .withColumn("order_status", F.lower(F.trim("order_status")))
            .withColumn("payment_status", F.lower(F.trim("payment_status")))
            .withColumn("payment_method", F.lower(F.trim("payment_method")))
            .withColumn("shipping_method", F.lower(F.trim("shipping_method")))
            .withColumn("delivery_city", F.initcap(F.trim("delivery_city")))
            .withColumn("delivery_country", F.initcap(F.trim("delivery_country")))
            .withColumn("delivery_postal_code", F.upper(F.trim("delivery_postal_code")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("order_date", F.to_date("order_date"))
            .withColumn("fulfillment_date", F.to_date("fulfillment_date"))
            .withColumn("delivery_date", F.to_date("delivery_date"))
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year and month for partitioning
            .withColumn("order_year", F.year("order_date"))
            .withColumn("order_month", F.month("order_date"))
            # Order age in days
            .withColumn(
                "order_age_days", F.datediff(F.current_date(), F.col("order_date"))
            )
            # Fulfillment time in days
            .withColumn(
                "fulfillment_time_days",
                F.datediff(F.col("fulfillment_date"), F.col("order_date")),
            )
            # Delivery time in days
            .withColumn(
                "delivery_time_days",
                F.datediff(F.col("delivery_date"), F.col("fulfillment_date")),
            )
            # Total shipping cost
            .withColumn("total_shipping_cost", F.round(F.col("shipping_cost"), 2))
            # Total discount amount
            .withColumn("total_discount_amount", F.round(F.col("discount_amount"), 2))
            # Total tax amount
            .withColumn("total_tax_amount", F.round(F.col("tax_amount"), 2))
            # Order value category
            .withColumn(
                "order_value_category",
                when(F.col("total_amount") < 50, "low")
                .when(F.col("total_amount") < 200, "medium")
                .when(F.col("total_amount") < 500, "high")
                .otherwise("premium"),
            )
            # Shipping cost category
            # .withColumn(
            #     "shipping_category",
            #     when(F.col("shipping_cost") == 0, "free")
            #     .when(F.col("shipping_cost") < 10, "economy")
            #     .when(F.col("shipping_cost") < 25, "standard")
            #     .otherwise("express"),
            # )
            # Customer segment category
            # .withColumn(
            #     "customer_tier",
            #     when(F.col("customer_segment") == "premium", "high")
            #     .when(F.col("customer_segment") == "regular", "medium")
            #     .otherwise("basic"),
            # )
            # Order status category
            # .withColumn(
            #     "order_status_category",
            #     when(
            #         F.col("order_status").isin(["completed", "delivered"]), "successful"
            #     )
            #     .when(F.col("order_status") == "cancelled", "cancelled")
            #     .otherwise("problematic"),
            # )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Validate order amounts
            .withColumn(
                "total_amount",
                when(F.col("total_amount") < 0, None).otherwise(
                    F.round(F.col("total_amount"), 2)
                ),
            )
            .withColumn(
                "subtotal",
                when(F.col("subtotal") < 0, None).otherwise(
                    F.round(F.col("subtotal"), 2)
                ),
            )
            .withColumn(
                "shipping_cost",
                when(F.col("shipping_cost") < 0, 0).otherwise(
                    F.round(F.col("shipping_cost"), 2)
                ),
            )
            .withColumn(
                "discount_amount",
                when(F.col("discount_amount") < 0, 0).otherwise(
                    F.round(F.col("discount_amount"), 2)
                ),
            )
            .withColumn(
                "tax_amount",
                when(F.col("tax_amount") < 0, 0).otherwise(
                    F.round(F.col("tax_amount"), 2)
                ),
            )
            # Validate percentages
            .withColumn(
                "discount_percentage",
                when(
                    (F.col("discount_percentage") < 0)
                    | (F.col("discount_percentage") > 1),
                    None,
                ).otherwise(F.round(F.col("discount_percentage"), 2)),
            )
            .withColumn(
                "tax_rate",
                when((F.col("tax_rate") < 0) | (F.col("tax_rate") > 1), None).otherwise(
                    F.round(F.col("tax_rate"), 2)
                ),
            )
            # Validate dates
            .withColumn(
                "order_date",
                when(F.col("order_date").isNull(), None).otherwise(F.col("order_date")),
            )
            # Validate customer ID
            .withColumn(
                "customer_id",
                when(F.col("customer_id").isNull(), -1).otherwise(F.col("customer_id")),
            )
            # ----------------------------
            # Data quality flags
            # ----------------------------
            # Flag orders with suspicious amounts
            # .withColumn(
            #     "amount_outlier_flag",
            #     when(
            #         (F.col("total_amount") > 5000)
            #         | (F.col("total_amount") < 5)
            #         | (F.col("shipping_cost") > 100),
            #         True,
            #     ).otherwise(False),
            # )
            # Flag orders with very high discounts
            .withColumn(
                "high_discount_flag",
                when(F.col("discount_percentage") > 0.5, True).otherwise(False),
            )
            # Flag orders with missing fulfillment dates
            .withColumn(
                "missing_fulfillment_flag",
                when(F.col("fulfillment_date").isNull(), True).otherwise(False),
            )
            # Flag orders with missing delivery dates
            .withColumn(
                "missing_delivery_flag",
                when(F.col("delivery_date").isNull(), True).otherwise(False),
            )
            # Flag orders with long fulfillment times
            .withColumn(
                "long_fulfillment_flag",
                when(F.col("fulfillment_time_days") > 14, True).otherwise(False),
            )
            # Flag orders with long delivery times
            .withColumn(
                "long_delivery_flag",
                when(F.col("delivery_time_days") > 30, True).otherwise(False),
            )
            # Flag cancelled orders with shipping costs
            .withColumn(
                "cancelled_with_shipping_flag",
                when(
                    (F.col("order_status") == "cancelled")
                    & (F.col("shipping_cost") > 0),
                    True,
                ).otherwise(False),
            )
            # Flag orders with invalid status combinations
            .withColumn(
                "status_inconsistency_flag",
                when(
                    (
                        (F.col("order_status") == "completed")
                        & (F.col("payment_status") != "paid")
                    )
                    | (
                        (F.col("order_status") == "cancelled")
                        & (F.col("payment_status") == "paid")
                    ),
                    True,
                ).otherwise(False),
            )
            # ----------------------------
            # Business logic validation
            # ----------------------------
            # Validate order total calculation
            # .withColumn(
            #     "total_calculation_valid",
            #     when(
            #         F.abs(
            #             F.col("total_amount")
            #             - (
            #                 F.col("subtotal")
            #                 - F.col("discount_amount")
            #                 + F.col("shipping_cost")
            #                 + F.col("tax_amount")
            #             )
            #         )
            #         > 0.01,
            #         False,
            #     ).otherwise(True),
            # )
            # Validate tax calculation
            # .withColumn(
            #     "tax_calculation_valid",
            #     when(
            #         F.abs(
            #             F.col("tax_amount")
            #             - (
            #                 (F.col("subtotal") - F.col("discount_amount"))
            #                 * F.col("tax_rate")
            #             )
            #         )
            #         > 0.01,
            #         False,
            #     ).otherwise(True),
            # )
            # Validate discount percentage
            #     .withColumn(
            #         "discount_percentage_valid",
            #         when(
            #             F.abs(
            #                 F.col("discount_amount")
            #                 - (F.col("subtotal") * F.col("discount_percentage"))
            #             )
            #             > 0.01,
            #             False,
            #         ).otherwise(True),
            #     )
        )

        # Write to cleaned bucket with partitioning
        df_clean.write.partitionBy("order_year", "order_month").mode(
            "overwrite"
        ).parquet(DESTINATION_PATH)

        print("Order data cleaning completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
