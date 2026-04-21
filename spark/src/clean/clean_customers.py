import os
import sys

# Get the absolute path to the spark directory
spark_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, spark_dir)

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as sf
import pyspark.sql.types as st
from delta.tables import DeltaTable

# from src.utils.config import RAW_PATH_CUSTOMERS, CLEAN_PATH_CUSTOMERS
from utils.config import RAW_PATH_CUSTOMERS, CLEAN_PATH_CUSTOMERS


def main():
    # Initialize Spark session
    # spark = (
    #     SparkSession.builder.appName("clean_customers")
    #     .master(os.environ.get("SPARK_MASTER", "spark://spark-master:7077"))
    #     .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)
    #     .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY)
    #     .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    #     .config("spark.hadoop.fs.s3a.path.style.access", "true")
    #     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #     # Delta Lake configurations
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config(
    #         "spark.sql.catalog.spark_catalog",
    #         "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     )
    #     .getOrCreate()
    # )
    spark = SparkSession.builder.appName("clean_customers").getOrCreate()

    try:
        # Read data from MinIO
        df = spark.read.json(RAW_PATH_CUSTOMERS)

        # Deduplicate exact records
        df_deduplicated = df.dropDuplicates()

        df_clean = (
            df_deduplicated
            # ----------------------------
            # Convert numeric columns
            # ----------------------------
            .withColumn("customer_id", sf.col("customer_id").cast("integer"))
            .withColumn("total_orders", sf.col("total_orders").cast("integer"))
            .withColumn(
                "total_spent", sf.col("total_spent").cast(st.DecimalType(12, 2))
            )
            # ----------------------------
            # Standardize / normalize text
            # ----------------------------
            .withColumn("email", sf.lower(sf.trim("email")))
            .withColumn("first_name", sf.initcap(sf.trim("first_name")))
            .withColumn("last_name", sf.initcap(sf.trim("last_name")))
            .withColumn("country_code", sf.upper(sf.trim("country_code")))
            .withColumn("city", sf.initcap(sf.trim("city")))
            .withColumn("country", sf.initcap(sf.trim("country")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("created_at", sf.to_timestamp("created_at"))
            .withColumn("updated_at", sf.to_timestamp("updated_at"))
            .withColumn("registration_date", sf.to_date("registration_date"))
            .withColumn("last_login_date", sf.to_date("last_login_date"))
            .withColumn("date_of_birth", sf.to_date("date_of_birth"))
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year of creation for partition
            .withColumn("creation_year", sf.year("created_at"))
            # Month of creation for partition
            .withColumn("creation_month", sf.month("created_at"))
            # Full address
            .withColumn(
                "full_address", sf.concat_ws(", ", "address", "city", "country")
            )
            # Full name
            .withColumn("name", sf.concat_ws(" ", "first_name", "last_name"))
            # Email domain
            .withColumn("email_domain", sf.split("email", "@").getItem(1))
            # Customer age
            .withColumn(
                "customer_age",
                sf.floor(sf.datediff(sf.current_date(), sf.col("date_of_birth")) / 365),
            )
            # Customer tenure
            .withColumn(
                "customer_tenure_days",
                sf.datediff(sf.current_date(), sf.col("registration_date")),
            )
            # Average spent per order
            .withColumn(
                "avg_spent_per_order",
                sf.when(
                    (sf.col("total_orders") > 0) & (sf.col("total_spent") >= 0),
                    sf.round(sf.col("total_spent") / sf.col("total_orders"), 2),
                ).cast(st.DecimalType(10, 2)),
            )
            # Days since last update
            .withColumn(
                "days_since_update",
                sf.datediff(sf.current_date(), sf.to_date("updated_at")),
            )
            # Days since last login
            .withColumn(
                "days_since_last_login",
                sf.datediff(sf.current_date(), sf.col("last_login_date")),
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Remove negative spending
            .withColumn(
                "total_spent",
                sf.when(sf.col("total_spent") < 0, None).otherwise(
                    sf.col("total_spent")
                ),
            )
            # Normalize phone
            .withColumn("phone_clean", sf.regexp_replace("phone", "[^0-9]", ""))
            # Flag inactive customers
            .withColumn(
                "inactive_customer",
                sf.when(sf.col("days_since_last_login") > 365, True).otherwise(False),
            )
            # ----------------------------
            # Add Preparation for SCD2
            # ----------------------------
            .withColumn("row_hash", sf.sha2("full_address", 256))
            .withColumn(
                "is_current",
                sf.row_number().over(
                    Window.partitionBy("customer_id").orderBy(sf.desc("created_at"))
                )
                == 1,
            )
            .withColumn("effective_from", sf.col("created_at"))
            # ----------------------------
            # Ingestion Timestamp
            # ----------------------------
            .withColumn("ingestion_timestamp", sf.current_timestamp())
        )

        # Write to cleaned bucket
        # df_clean.write.format("delta").partitionBy(
        #     "creation_year", "creation_month"
        # ).mode("overwrite").save(DESTINATION_PATH)

        # merge into cleaned bucket
        delta_table_exists = DeltaTable.isDeltaTable(spark, CLEAN_PATH_CUSTOMERS)

        if not delta_table_exists:
            df_clean.write.format("delta").partitionBy(
                "creation_year", "creation_month"
            ).mode("overwrite").save(CLEAN_PATH_CUSTOMERS)
        else:
            cleaned_table = DeltaTable.forPath(spark, CLEAN_PATH_CUSTOMERS)
            (
                cleaned_table.alias("tgt")
                .merge(
                    df_clean.alias("src"),
                    "tgt.customer_id = src.customer_id AND tgt.created_at = src.created_at",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

        print("Data processing completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        # spark.stop()
        pass


if __name__ == "__main__":
    main()
