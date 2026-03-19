from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, current_date
import json
import os
from pyspark.sql import functions as F

ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "forge-commerce-user")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "forge-commerce-pass")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
PREFIX = "customers"
ORIGIN_BUCKET = "raw"
DESTINATION_BUCKET = "cleaned"
ORIGIN_PATH = f"s3a://{ORIGIN_BUCKET}/{PREFIX}/"
DESTINATION_PATH = f"s3a://{DESTINATION_BUCKET}/{PREFIX}/"


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("ReadCustomersFromRaw")
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

    try:
        # Read data from MinIO
        df = spark.read.json(ORIGIN_PATH)

        # Drop duplicates based on customer id and created at
        df_no_dup = df.dropDuplicates(["customer_id", "created_at"])

        df_clean = (
            df_no_dup
            # ----------------------------
            # Standardize / normalize text
            # ----------------------------
            .withColumn("email", F.lower(F.trim("email")))
            .withColumn("first_name", F.initcap(F.trim("first_name")))
            .withColumn("last_name", F.initcap(F.trim("last_name")))
            .withColumn("country_code", F.upper(F.trim("country_code")))
            .withColumn("city", F.initcap(F.trim("city")))
            .withColumn("country", F.initcap(F.trim("country")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("created_at", F.to_timestamp("created_at"))
            .withColumn("updated_at", F.to_timestamp("updated_at"))
            .withColumn("registration_date", F.to_date("registration_date"))
            .withColumn("last_login_date", F.to_date("last_login_date"))
            .withColumn("date_of_birth", F.to_date("date_of_birth"))
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year of creation for partition
            .withColumn("creation_year", F.year("created_at"))
            # Month of creation for partition
            .withColumn("creation_month", F.month("created_at"))
            # Full address
            .withColumn("full_address", F.concat_ws(", ", "address", "city", "country"))
            # Full name
            .withColumn("name", F.concat_ws(" ", "first_name", "last_name"))
            # Email domain
            .withColumn("email_domain", F.split("email", "@").getItem(1))
            # Customer age
            .withColumn(
                "customer_age",
                F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365),
            )
            # Customer tenure
            .withColumn(
                "customer_tenure_days",
                F.datediff(F.current_date(), F.col("registration_date")),
            )
            # Average spent per order
            .withColumn(
                "avg_spent_per_order",
                F.when(
                    F.col("total_orders") > 0,
                    F.round(F.col("total_spent") / F.col("total_orders"), 2),
                ),
            )
            # Days since last update
            .withColumn(
                "days_since_update",
                F.datediff(F.current_date(), F.to_date("updated_at")),
            )
            # Days since last login
            .withColumn(
                "days_since_last_login",
                F.datediff(F.current_date(), F.col("last_login_date")),
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Remove negative spending
            .withColumn(
                "total_spent",
                F.when(F.col("total_spent") < 0, None).otherwise(F.col("total_spent")),
            )
            # Normalize phone
            .withColumn("phone_clean", F.regexp_replace("phone", "[^0-9]", ""))
            # Flag inactive customers
            .withColumn(
                "inactive_customer",
                F.when(F.col("days_since_last_login") > 365, True).otherwise(False),
            )
        )

        # Write to cleaned bucket
        df_clean.write.format("delta").partitionBy(
            "creation_year", "creation_month"
        ).mode("overwrite").save(DESTINATION_PATH)

        print("Data processing completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
