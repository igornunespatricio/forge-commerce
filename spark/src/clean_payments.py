from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, current_date
import json
import os
from pyspark.sql import functions as F

ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "forge-commerce-user")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "forge-commerce-pass")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
PREFIX = "payments"
ORIGIN_BUCKET = "raw"
DESTINATION_BUCKET = "cleaned"
ORIGIN_PATH = f"s3a://{ORIGIN_BUCKET}/{PREFIX}/"
DESTINATION_PATH = f"s3a://{DESTINATION_BUCKET}/{PREFIX}/"


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("ReadPaymentsFromRaw")
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
            .withColumn("payment_method", F.lower(F.trim("payment_method")))
            .withColumn("payment_status", F.lower(F.trim("payment_status")))
            .withColumn("payment_gateway", F.lower(F.trim("payment_gateway")))
            .withColumn("currency_code", F.upper(F.trim("currency_code")))
            .withColumn("customer_segment", F.lower(F.trim("customer_segment")))
            .withColumn("chargeback_reason", F.lower(F.trim("chargeback_reason")))
            .withColumn("payment_reference", F.upper(F.trim("payment_reference")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("payment_date", F.to_date("payment_date"))
            .withColumn("chargeback_date", F.to_date("chargeback_date"))
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year and month for partitioning
            .withColumn("payment_year", F.year("payment_date"))
            .withColumn("payment_month", F.month("payment_date"))
            # Payment age in days
            .withColumn(
                "payment_age_days", F.datediff(F.current_date(), F.col("payment_date"))
            )
            # Chargeback time in days
            .withColumn(
                "chargeback_time_days",
                F.datediff(F.col("chargeback_date"), F.col("payment_date")),
            )
            # Payment success flag
            .withColumn(
                "is_successful_payment",
                when(F.col("payment_status") == "success", True).otherwise(False),
            )
            # Payment failure flag
            .withColumn(
                "is_failed_payment",
                when(F.col("payment_status") == "failed", True).otherwise(False),
            )
            # Payment pending flag
            .withColumn(
                "is_pending_payment",
                when(F.col("payment_status") == "pending", True).otherwise(False),
            )
            # Chargeback flag
            .withColumn(
                "has_chargeback",
                when(F.col("chargeback_amount") > 0, True).otherwise(False),
            )
            # Fraud risk level
            .withColumn(
                "fraud_risk_level",
                when(F.col("fraud_score") >= 0.8, "high")
                .when(F.col("fraud_score") >= 0.5, "medium")
                .when(F.col("fraud_score") >= 0.2, "low")
                .otherwise("minimal"),
            )
            # Payment method category
            .withColumn(
                "payment_method_category",
                when(
                    F.col("payment_method").isin(["credit_card", "debit_card"]), "card"
                )
                .when(
                    F.col("payment_method").isin(["paypal", "stripe"]), "digital_wallet"
                )
                .otherwise("other"),
            )
            # Transaction fee category
            .withColumn(
                "fee_category",
                when(F.col("transaction_fee") == 0, "free")
                .when(F.col("transaction_fee") < 1, "low")
                .when(F.col("transaction_fee") < 5, "medium")
                .otherwise("high"),
            )
            # Order amount category
            .withColumn(
                "order_amount_category",
                when(F.col("order_amount") < 50, "small")
                .when(F.col("order_amount") < 200, "medium")
                .when(F.col("order_amount") < 1000, "large")
                .otherwise("premium"),
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Validate order amounts
            .withColumn(
                "order_amount",
                when(F.col("order_amount") < 0, None).otherwise(
                    F.round(F.col("order_amount"), 2)
                ),
            )
            # Validate transaction fees
            .withColumn(
                "transaction_fee",
                when(F.col("transaction_fee") < 0, 0).otherwise(
                    F.round(F.col("transaction_fee"), 2)
                ),
            )
            # Validate net amounts
            .withColumn(
                "net_amount",
                when(F.col("net_amount") < 0, None).otherwise(
                    F.round(F.col("net_amount"), 2)
                ),
            )
            # Validate chargeback amounts
            .withColumn(
                "chargeback_amount",
                when(F.col("chargeback_amount") < 0, 0).otherwise(
                    F.round(F.col("chargeback_amount"), 2)
                ),
            )
            # Validate fraud scores
            .withColumn(
                "fraud_score",
                when(
                    (F.col("fraud_score") < 0) | (F.col("fraud_score") > 1), None
                ).otherwise(F.round(F.col("fraud_score"), 3)),
            )
            # Validate transaction fee rates
            .withColumn(
                "transaction_fee_rate",
                when(
                    (F.col("transaction_fee_rate") < 0)
                    | (F.col("transaction_fee_rate") > 1),
                    None,
                ).otherwise(F.round(F.col("transaction_fee_rate"), 4)),
            )
            # Validate dates
            .withColumn(
                "payment_date",
                when(F.col("payment_date").isNull(), None).otherwise(
                    F.col("payment_date")
                ),
            )
            # Validate customer ID
            .withColumn(
                "customer_id",
                when(F.col("customer_id").isNull(), -1).otherwise(F.col("customer_id")),
            )
            # Validate order ID
            .withColumn(
                "order_id",
                when(F.col("order_id").isNull(), -1).otherwise(F.col("order_id")),
            )
            # ----------------------------
            # Data quality flags
            # ----------------------------
            # Flag payments with suspicious amounts
            # .withColumn(
            #     "amount_outlier_flag",
            #     when(
            #         (F.col("order_amount") > 10000)
            #         | (F.col("order_amount") < 1)
            #         | (F.col("transaction_fee") > 500),
            #         True,
            #     ).otherwise(False),
            # )
            # Flag high fraud scores
            .withColumn(
                "high_fraud_score_flag",
                when(F.col("fraud_score") > 0.8, True).otherwise(False),
            )
            # Flag payments with missing payment dates
            .withColumn(
                "missing_payment_date_flag",
                when(F.col("payment_date").isNull(), True).otherwise(False),
            )
            # Flag payments with missing chargeback dates
            .withColumn(
                "missing_chargeback_date_flag",
                when(
                    (F.col("chargeback_amount") > 0)
                    & F.col("chargeback_date").isNull(),
                    True,
                ).otherwise(False),
            )
            # Flag payments with long chargeback times
            .withColumn(
                "long_chargeback_time_flag",
                when(F.col("chargeback_time_days") > 180, True).otherwise(False),
            )
            # Flag payments with negative net amounts
            .withColumn(
                "negative_net_amount_flag",
                when(F.col("net_amount") < 0, True).otherwise(False),
            )
            # Flag payments with invalid status combinations
            .withColumn(
                "status_inconsistency_flag",
                when(
                    (
                        (F.col("payment_status") == "success")
                        & (F.col("chargeback_amount") > 0)
                    )
                    | (
                        (F.col("payment_status") == "failed")
                        & (F.col("chargeback_amount") > 0)
                    ),
                    True,
                ).otherwise(False),
            )
            # Flag payments with mismatched amounts
            # .withColumn(
            #     "amount_mismatch_flag",
            #     when(
            #         F.abs(
            #             F.col("net_amount")
            #             - (F.col("order_amount") - F.col("transaction_fee"))
            #         )
            #         > 0.01,
            #         True,
            #     ).otherwise(False),
            # )
            # ----------------------------
            # Business logic validation
            # ----------------------------
            # Validate payment status consistency
            .withColumn(
                "payment_status_valid",
                when(
                    F.col("payment_status").isin(
                        ["success", "failed", "pending", "refunded"]
                    ),
                    True,
                ).otherwise(False),
            )
            # Validate payment method consistency
            .withColumn(
                "payment_method_valid",
                when(
                    F.col("payment_method").isin(
                        [
                            "credit_card",
                            "debit_card",
                            "paypal",
                            "apple_pay",
                            "google_pay",
                        ]
                    ),
                    True,
                ).otherwise(False),
            )
            # Validate currency code consistency
            # .withColumn(
            #     "currency_valid",
            #     when(
            #         F.col("currency_code").isin(["USD", "EUR", "GBP", "CAD", "AUD"]),
            #         True,
            #     ).otherwise(False),
            # )
            # Validate chargeback reason consistency
            .withColumn(
                "chargeback_reason_valid",
                when(
                    F.col("chargeback_reason").isNull()
                    | F.col("chargeback_reason").isin(
                        ["fraudulent", "not_received", "defective", "cancelled"]
                    ),
                    True,
                ).otherwise(False),
            )
            # Validate transaction fee calculation
            # .withColumn(
            #     "fee_calculation_valid",
            #     when(
            #         F.abs(
            #             F.col("transaction_fee")
            #             - (F.col("order_amount") * F.col("transaction_fee_rate"))
            #         )
            #         > 0.01,
            #         False,
            #     ).otherwise(True),
            # )
        )

        # Write to cleaned bucket with partitioning
        df_clean.write.partitionBy("payment_year", "payment_month").mode(
            "overwrite"
        ).parquet(DESTINATION_PATH)

        print("Payment data cleaning completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
