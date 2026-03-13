from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, current_date
import json


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("Delta Lakehouse on MinIO")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    try:
        # Read data from MinIO
        df = spark.read.json(
            "s3a://raw/customers/customers_batch_0000_20260309_092216.json"
        )

        # Apply transformations
        cleaned_df = (
            df.filter(col("is_active") == True)
            .withColumn("age", datediff(current_date(), col("date_of_birth")) / 365)
            .withColumn(
                "customer_tier",
                when(col("customer_lifetime_value") > 1000, "premium")
                .when(col("customer_lifetime_value") > 500, "medium")
                .otherwise("basic"),
            )
        )

        # Write to cleaned bucket
        cleaned_df.write.mode("overwrite").parquet("s3a://cleaned/customers")

        print("Data processing completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
