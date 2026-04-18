import os
import sys

# Get the absolute path to the spark directory
spark_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, spark_dir)

from delta import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from src.utils.config import RAW_PATH_PRODUCTS, CLEAN_PATH_PRODUCTS


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("clean_products")
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
        df = spark.read.json(RAW_PATH_PRODUCTS)

        # Deduplicate exact records
        df_deduplicated = df.dropDuplicates()

        df_clean = (
            df_deduplicated
            # ----------------------------
            # Standardize / normalize text
            # ----------------------------
            .withColumn("product_name", sf.initcap(sf.trim("product_name")))
            .withColumn("category", sf.lower(sf.trim("category")))
            .withColumn("subcategory", sf.lower(sf.trim("subcategory")))
            .withColumn("brand", sf.initcap(sf.trim("brand")))
            .withColumn("description", sf.trim("description"))
            .withColumn("supplier_name", sf.initcap(sf.trim("supplier_name")))
            .withColumn("supplier_country", sf.upper(sf.trim("supplier_country")))
            .withColumn("color", sf.lower(sf.trim("color")))
            .withColumn("material", sf.lower(sf.trim("material")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("created_at", sf.to_timestamp("created_at"))
            .withColumn("last_updated", sf.to_timestamp("last_updated"))
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year of creation for partition
            .withColumn("creation_year", sf.year("created_at"))
            # Month of creation for partition
            .withColumn("creation_month", sf.month("created_at"))
            # Full category path
            .withColumn("full_category", sf.concat_ws(" > ", "category", "subcategory"))
            # Profit per unit
            .withColumn(
                "profit_per_unit", sf.round(sf.col("price") - sf.col("cost_price"), 2)
            )
            # Total profit potential
            .withColumn(
                "total_profit_potential",
                sf.round(sf.col("profit_per_unit") * sf.col("inventory_level"), 2),
            )
            # Product age in days
            .withColumn(
                "product_age_days", sf.datediff(sf.current_date(), sf.col("created_at"))
            )
            # Days since last update
            .withColumn(
                "days_since_update",
                sf.datediff(sf.current_date(), sf.col("last_updated")),
            )
            # Price category
            .withColumn(
                "price_category",
                sf.when(sf.col("price") < 25, "budget")
                .when(sf.col("price") < 100, "mid-range")
                .when(sf.col("price") < 500, "premium")
                .otherwise("luxury"),
            )
            # Weight category
            .withColumn(
                "weight_category",
                sf.when(sf.col("weight") < 1, "light")
                .when(sf.col("weight") < 5, "medium")
                .when(sf.col("weight") < 15, "heavy")
                .otherwise("very_heavy"),
            )
            # Dimensions parsing
            .withColumn("dimensions_parsed", sf.split("dimensions", "x"))
            .withColumn(
                "length_cm", sf.col("dimensions_parsed").getItem(0).cast("double")
            )
            .withColumn(
                "width_cm", sf.col("dimensions_parsed").getItem(1).cast("double")
            )
            .withColumn(
                "height_cm", sf.col("dimensions_parsed").getItem(2).cast("double")
            )
            .withColumn(
                "volume_cm3",
                sf.round(
                    sf.col("length_cm") * sf.col("width_cm") * sf.col("height_cm"), 2
                ),
            )
            .withColumn(
                "dimensions_ratio",
                sf.round(sf.col("length_cm") / sf.col("height_cm"), 2),
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # Recalculate margin if invalid
            .withColumn(
                "margin",
                sf.when(
                    (sf.col("margin") <= 0) | (sf.col("margin") > 1),
                    sf.round(
                        (sf.col("price") - sf.col("cost_price")) / sf.col("price"), 2
                    ),
                ).otherwise(sf.round(sf.col("margin"), 2)),
            )
            # Validate rating
            .withColumn(
                "product_rating",
                sf.when(
                    (sf.col("product_rating") < 0) | (sf.col("product_rating") > 5),
                    None,
                ).otherwise(sf.round(sf.col("product_rating"), 1)),
            )
            # Validate review count
            .withColumn(
                "review_count",
                sf.when(sf.col("review_count") < 0, 0).otherwise(
                    sf.col("review_count")
                ),
            )
            # Validate weight
            .withColumn(
                "weight",
                sf.when(sf.col("weight") < 0, None).otherwise(
                    sf.round(sf.col("weight"), 2)
                ),
            )
            # ----------------------------
            # Data quality flags
            # ----------------------------
            # Flag low stock items
            .withColumn(
                "low_stock_flag",
                sf.when(sf.col("inventory_level") < 10, True).otherwise(False),
            )
            # Flag old products (not updated in 6 months)
            .withColumn(
                "old_product_flag",
                sf.when(sf.col("days_since_update") > 180, True).otherwise(False),
            )
            # Flag products with very low ratings
            .withColumn(
                "low_rating_flag",
                sf.when(sf.col("product_rating") < 2.5, True).otherwise(False),
            )
            # Flag discontinued but still active
            .withColumn(
                "status_inconsistency_flag",
                sf.when(
                    (sf.col("is_discontinued") == True) & (sf.col("is_active") == True),
                    True,
                ).otherwise(False),
            )
            # Flag products with invalid dimensions
            .withColumn(
                "invalid_dimensions_flag",
                sf.when(
                    (sf.size(sf.split("dimensions", "x")) != 3)
                    | (sf.col("length_cm") <= 0)
                    | (sf.col("width_cm") <= 0)
                    | (sf.col("height_cm") <= 0),
                    True,
                ).otherwise(False),
            )
        )

        # Write to cleaned bucket with partitioning
        # df_clean.write.format("delta").partitionBy(
        #     "creation_year", "creation_month"
        # ).mode("overwrite").save(DESTINATION_PATH)

        delta_table_exists = DeltaTable.isDeltaTable(spark, CLEAN_PATH_PRODUCTS)
        if not delta_table_exists:
            df_clean.write.format("delta").partitionBy(
                "creation_year", "creation_month"
            ).mode("overwrite").save(CLEAN_PATH_PRODUCTS)
        else:
            cleaned_table = DeltaTable.forPath(spark, CLEAN_PATH_PRODUCTS)
            (
                cleaned_table.alias("tgt")
                .merge(
                    df_clean.alias("src"),
                    "tgt.product_id = src.product_id AND tgt.created_at = src.created_at",
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
