import os
import sys

# Get the absolute path to the spark directory
spark_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, spark_dir)

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
import pyspark.sql.types as st
import json
from src.utils.config import RAW_PATH_PAYMENTS, CLEAN_PATH_PAYMENTS


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("clean_payments")
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
        df = spark.read.json(RAW_PATH_PAYMENTS)

        # Drop duplicates based on payment id and created at
        df_deduplicated = df.dropDuplicates()

        df_clean = (
            df_deduplicated
            # ----------------------------
            # Standardize / normalize text
            # ----------------------------
            .withColumn("payment_method", sf.lower(sf.trim("payment_method")))
            .withColumn("payment_status", sf.lower(sf.trim("payment_status")))
            .withColumn("payment_gateway", sf.lower(sf.trim("payment_gateway")))
            .withColumn("currency_code", sf.upper(sf.trim("currency_code")))
            .withColumn("customer_segment", sf.lower(sf.trim("customer_segment")))
            .withColumn("chargeback_reason", sf.lower(sf.trim("chargeback_reason")))
            .withColumn("payment_reference", sf.upper(sf.trim("payment_reference")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            # combine date and time into payment timestamp
            .withColumn(
                "payment_timestamp",
                sf.to_timestamp(
                    sf.concat_ws(
                        " ",
                        sf.col("payment_date"),
                        sf.col("payment_time"),
                    ),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            )
            .withColumn("chargeback_date", sf.to_date("chargeback_date"))
            .withColumn("created_at", sf.to_timestamp("created_at"))
            .withColumn("updated_at", sf.to_timestamp("updated_at"))
            # ----------------------------
            # Convert numeric columns
            # ----------------------------
            .withColumn("payment_id", sf.col("payment_id").cast("integer"))
            .withColumn("customer_id", sf.col("customer_id").cast("integer"))
            .withColumn("order_id", sf.col("order_id").cast("integer"))
            .withColumn(
                "order_amount", sf.col("order_amount").cast(st.DecimalType(10, 2))
            )
            .withColumn(
                "transaction_fee_rate",
                sf.col("transaction_fee_rate").cast(st.DecimalType(10, 2)),
            )
            .withColumn(
                "transaction_fee", sf.col("transaction_fee").cast(st.DecimalType(10, 2))
            )
            .withColumn("net_amount", sf.col("net_amount").cast(st.DecimalType(10, 2)))
            .withColumn(
                "fraud_score", sf.col("fraud_score").cast(st.DecimalType(10, 2))
            )
            .withColumn(
                "chargeback_amount",
                sf.col("chargeback_amount").cast(st.DecimalType(10, 2)),
            )
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year and month for partitioning
            .withColumn("payment_year", sf.year("payment_date"))
            .withColumn("payment_month", sf.month("payment_date"))
            # Chargeback time in days
            .withColumn(
                "chargeback_time_days",
                sf.datediff(sf.col("chargeback_date"), sf.col("payment_date")),
            )
            # Payment success flag
            .withColumn(
                "is_successful_payment",
                sf.when(sf.col("payment_status") == "success", True).otherwise(False),
            )
            # Payment failure flag
            .withColumn(
                "is_failed_payment",
                sf.when(sf.col("payment_status") == "failed", True).otherwise(False),
            )
            # Payment pending flag
            .withColumn(
                "is_pending_payment",
                sf.when(sf.col("payment_status") == "pending", True).otherwise(False),
            )
            # Chargeback flag
            .withColumn(
                "has_chargeback",
                sf.when(sf.col("chargeback_amount") > 0, True).otherwise(False),
            )
            # Fraud risk level
            .withColumn(
                "fraud_risk_level",
                sf.when(sf.col("fraud_score") >= 0.8, "high")
                .when(sf.col("fraud_score") >= 0.5, "medium")
                .when(sf.col("fraud_score") >= 0.2, "low")
                .otherwise("minimal"),
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Validate order amounts
            .withColumn(
                "order_amount",
                sf.when(sf.col("order_amount") < 0, None).otherwise(
                    sf.col("order_amount")
                ),
            )
            # Validate transaction fees
            .withColumn(
                "transaction_fee",
                sf.when(sf.col("transaction_fee") < 0, 0).otherwise(
                    sf.col("transaction_fee")
                ),
            )
            # Validate net amounts
            .withColumn(
                "net_amount",
                sf.when(sf.col("net_amount") < 0, None).otherwise(sf.col("net_amount")),
            )
            # Validate chargeback amounts
            .withColumn(
                "chargeback_amount",
                sf.when(sf.col("chargeback_amount") < 0, 0).otherwise(
                    sf.col("chargeback_amount")
                ),
            )
            # Validate fraud scores
            .withColumn(
                "fraud_score",
                sf.when(
                    (sf.col("fraud_score") < 0) | (sf.col("fraud_score") > 1), None
                ).otherwise(sf.col("fraud_score")),
            )
            # Validate transaction fee rates
            .withColumn(
                "transaction_fee_rate",
                sf.when(
                    (sf.col("transaction_fee_rate") < 0)
                    | (sf.col("transaction_fee_rate") > 1),
                    None,
                ).otherwise(sf.col("transaction_fee_rate")),
            )
            # ----------------------------
            # Data quality flags
            # ----------------------------
            # Flag payments with missing payment dates
            .withColumn(
                "missing_payment_date_flag",
                sf.when(sf.col("payment_date").isNull(), True).otherwise(False),
            )
            # Flag payments with missing chargeback dates
            .withColumn(
                "missing_chargeback_date_flag",
                sf.when(
                    (sf.col("chargeback_amount") > 0)
                    & sf.col("chargeback_date").isNull(),
                    True,
                ).otherwise(False),
            )
            # Flag payments with long chargeback times
            .withColumn(
                "long_chargeback_time_flag",
                sf.when(sf.col("chargeback_time_days") > 180, True).otherwise(False),
            )
            # Flag payments with negative net amounts
            .withColumn(
                "negative_net_amount_flag",
                sf.when(sf.col("net_amount") < 0, True).otherwise(False),
            )
            # Flag payments with invalid status combinations
            .withColumn(
                "status_inconsistency_flag",
                sf.when(
                    (
                        (sf.col("payment_status") == "success")
                        & (sf.col("chargeback_amount") > 0)
                    )
                    | (
                        (sf.col("payment_status") == "failed")
                        & (sf.col("chargeback_amount") > 0)
                    ),
                    True,
                ).otherwise(False),
            )
        )

        # Write to cleaned bucket with partitioning
        # df_clean.write.format("delta").partitionBy(
        #     "payment_year", "payment_month"
        # ).mode("overwrite").save(DESTINATION_PATH)

        # merge into cleaned bucket
        detal_table_exists = DeltaTable.isDeltaTable(spark, CLEAN_PATH_PAYMENTS)
        if not detal_table_exists:
            df_clean.write.format("delta").partitionBy(
                "payment_year", "payment_month"
            ).mode("overwrite").save(CLEAN_PATH_PAYMENTS)
        else:
            cleaned_table = DeltaTable.forPath(spark, CLEAN_PATH_PAYMENTS)
            (
                cleaned_table.alias("tgt")
                .merge(
                    df_clean.alias("src"),
                    "tgt.payment_id = src.payment_id AND tgt.created_at = src.created_at",
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
