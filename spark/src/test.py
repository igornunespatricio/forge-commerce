#!/usr/bin/env python3
"""
Simple test script to read customer data from S3A and save as Parquet
"""

from pyspark.sql import SparkSession


def main():
    # Create Spark session with MinIO configuration
    spark = SparkSession.builder.appName("CustomerDataTest").getOrCreate()

    try:
        print("Reading customer data from S3A...")

        # Read the customer data
        df = spark.read.json(
            "s3a://raw/customers/customers_batch_0000_20260309_092216.json"
        )

        print(f"Loaded {df.count()} customer records")
        print("Schema:")
        df.printSchema()

        print("\nSample data:")
        df.show(5, truncate=False)

        # Write to Parquet format
        output_path = "s3a://cleaned/customers"
        print(f"\nWriting data to {output_path}...")

        df.write.mode("overwrite").parquet(output_path)

        print("Data successfully written to Parquet format!")

        # Verify the write by reading it back
        print("\nVerifying the write operation...")
        df_read_back = spark.read.parquet(output_path)
        print(f"Read back {df_read_back.count()} records from Parquet")
        df_read_back.show(5, truncate=False)

    except Exception as e:
        print(f"Error: {e}")
        spark.stop()
        return 1

    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    exit(main())
