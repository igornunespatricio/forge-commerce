from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, current_date
import json
import os
from pyspark.sql import functions as F

ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "forge-commerce-user")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "forge-commerce-pass")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")
PREFIX = "products"
ORIGIN_BUCKET = "raw"
DESTINATION_BUCKET = "cleaned"
ORIGIN_PATH = f"s3a://{ORIGIN_BUCKET}/{PREFIX}/"
DESTINATION_PATH = f"s3a://{DESTINATION_BUCKET}/{PREFIX}/"


def main():
    # Initialize Spark session
    spark = (
        SparkSession.builder.appName("ReadProductsFromRaw")
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
            .withColumn("product_name", F.initcap(F.trim("product_name")))
            .withColumn("category", F.lower(F.trim("category")))
            .withColumn("subcategory", F.lower(F.trim("subcategory")))
            .withColumn("brand", F.initcap(F.trim("brand")))
            .withColumn("description", F.trim("description"))
            .withColumn("supplier_name", F.initcap(F.trim("supplier_name")))
            .withColumn("supplier_country", F.upper(F.trim("supplier_country")))
            .withColumn("color", F.lower(F.trim("color")))
            .withColumn("material", F.lower(F.trim("material")))
            # ----------------------------
            # Convert date columns
            # ----------------------------
            .withColumn("created_at", F.to_date("created_at"))
            .withColumn("last_updated", F.to_date("last_updated"))
            # ----------------------------
            # Derived fields
            # ----------------------------
            # Year of creation for partition
            .withColumn("creation_year", F.year("created_at"))
            # Month of creation for partition
            .withColumn("creation_month", F.month("created_at"))
            # Full category path
            .withColumn("full_category", F.concat_ws(" > ", "category", "subcategory"))
            # Profit per unit
            .withColumn(
                "profit_per_unit", F.round(F.col("price") - F.col("cost_price"), 2)
            )
            # Total profit potential
            .withColumn(
                "total_profit_potential",
                F.round(F.col("profit_per_unit") * F.col("inventory_level"), 2),
            )
            # Product age in days
            .withColumn(
                "product_age_days", F.datediff(F.current_date(), F.col("created_at"))
            )
            # Days since last update
            .withColumn(
                "days_since_update", F.datediff(F.current_date(), F.col("last_updated"))
            )
            # Inventory turnover rate (estimated)
            .withColumn(
                "estimated_monthly_sales", F.round(F.col("inventory_level") / 6, 0)
            )  # Rough estimate
            # Price category
            .withColumn(
                "price_category",
                when(F.col("price") < 25, "budget")
                .when(F.col("price") < 100, "mid-range")
                .when(F.col("price") < 500, "premium")
                .otherwise("luxury"),
            )
            # Weight category
            .withColumn(
                "weight_category",
                when(F.col("weight") < 1, "light")
                .when(F.col("weight") < 5, "medium")
                .when(F.col("weight") < 15, "heavy")
                .otherwise("very_heavy"),
            )
            # Dimensions parsing
            .withColumn("dimensions_parsed", F.split("dimensions", "x"))
            .withColumn(
                "length_cm", F.col("dimensions_parsed").getItem(0).cast("double")
            )
            .withColumn(
                "width_cm", F.col("dimensions_parsed").getItem(1).cast("double")
            )
            .withColumn(
                "height_cm", F.col("dimensions_parsed").getItem(2).cast("double")
            )
            .withColumn(
                "volume_cm3",
                F.round(F.col("length_cm") * F.col("width_cm") * F.col("height_cm"), 2),
            )
            .withColumn(
                "dimensions_ratio", F.round(F.col("length_cm") / F.col("height_cm"), 2)
            )
            # ----------------------------
            # Data quality improvements
            # ----------------------------
            # # Validate and clean price
            # .withColumn(
            #     "price",
            #     when(F.col("price") <= 0, None)
            #     .when(
            #         F.col("price") > 10000, F.round(F.col("price") / 100, 2)
            #     )  # Fix potential decimal issues
            #     .otherwise(F.round(F.col("price"), 2)),
            # )
            # Validate and clean cost price
            # .withColumn(
            #     "cost_price",
            #     when(F.col("cost_price") <= 0, None)
            #     .when(
            #         F.col("cost_price") > F.col("price"),
            #         F.round(F.col("price") * 0.7, 2),
            #     )  # Set to 70% of price if higher
            #     .otherwise(F.round(F.col("cost_price"), 2)),
            # )
            # Recalculate margin if invalid
            .withColumn(
                "margin",
                when(
                    (F.col("margin") <= 0) | (F.col("margin") > 1),
                    F.round((F.col("price") - F.col("cost_price")) / F.col("price"), 2),
                ).otherwise(F.round(F.col("margin"), 2)),
            )
            # Validate inventory level
            # .withColumn(
            #     "inventory_level",
            #     when(F.col("inventory_level") < 0, 0)
            #     .when(
            #         F.col("inventory_level") > 100000, 100000
            #     )  # Cap extremely high values
            #     .otherwise(F.col("inventory_level")),
            # )
            # Validate rating
            .withColumn(
                "product_rating",
                when(
                    (F.col("product_rating") < 1) | (F.col("product_rating") > 5), None
                ).otherwise(F.round(F.col("product_rating"), 1)),
            )
            # Validate review count
            .withColumn(
                "review_count",
                when(F.col("review_count") < 0, 0).otherwise(F.col("review_count")),
            )
            # Validate weight
            .withColumn(
                "weight",
                when(F.col("weight") < 0, None)
                # .when(F.col("weight") > 100, 25.0)  # Cap extremely heavy items
                .otherwise(F.round(F.col("weight"), 2)),
            )
            # ----------------------------
            # Data quality flags
            # ----------------------------
            # Flag products with suspicious pricing
            .withColumn(
                "price_outlier_flag",
                when(
                    (F.col("price") > 5000)
                    | (F.col("price") < 1)
                    | (F.col("margin") < 0.1)
                    | (F.col("margin") > 0.9),
                    True,
                ).otherwise(False),
            )
            # Flag low stock items
            .withColumn(
                "low_stock_flag",
                when(F.col("inventory_level") < 10, True).otherwise(False),
            )
            # Flag old products (not updated in 6 months)
            .withColumn(
                "old_product_flag",
                when(F.col("days_since_update") > 180, True).otherwise(False),
            )
            # Flag products with very low ratings
            .withColumn(
                "low_rating_flag",
                when(F.col("product_rating") < 2.5, True).otherwise(False),
            )
            # Flag products with no reviews but high inventory (potential issues)
            # .withColumn(
            #     "no_reviews_high_stock_flag",
            #     when(
            #         (F.col("review_count") == 0) & (F.col("inventory_level") > 1000),
            #         True,
            #     ).otherwise(False),
            # )
            # Flag discontinued but still active
            .withColumn(
                "status_inconsistency_flag",
                when(
                    (F.col("is_discontinued") == True) & (F.col("is_active") == True),
                    True,
                ).otherwise(False),
            )
            # Flag products with invalid dimensions
            .withColumn(
                "invalid_dimensions_flag",
                when(
                    (F.size(F.split("dimensions", "x")) != 3)
                    | (F.col("length_cm") <= 0)
                    | (F.col("width_cm") <= 0)
                    | (F.col("height_cm") <= 0),
                    True,
                ).otherwise(False),
            )
            # ----------------------------
            # Business logic validation
            # ----------------------------
            # Validate margin calculation consistency
            # .withColumn(
            #     "margin_consistency_check",
            #     when(
            #         F.abs(
            #             (F.col("price") - F.col("cost_price")) / F.col("price")
            #             - F.col("margin")
            #         )
            #         > 0.01,
            #         False,
            #     ).otherwise(True),
            # )
            # Category-appropriate pricing validation
            # .withColumn(
            #     "category_price_consistency",
            #     when(
            #         ((F.col("category") == "electronics") & (F.col("price") < 10))
            #         | ((F.col("category") == "books_media") & (F.col("price") > 200))
            #         | ((F.col("category") == "toys_games") & (F.col("price") > 500)),
            #         False,
            #     ).otherwise(True),
            # )
            # Inventory level by category validation
            #     .withColumn(
            #         "category_inventory_consistency",
            #         when(
            #             (
            #                 (F.col("category") == "electronics")
            #                 & (F.col("inventory_level") > 5000)
            #             )
            #             | (
            #                 (F.col("category") == "books_media")
            #                 & (F.col("inventory_level") < 50)
            #             ),
            #             False,
            #         ).otherwise(True),
            #     )
        )

        # Write to cleaned bucket with partitioning
        df_clean.write.partitionBy("creation_year", "creation_month").mode(
            "overwrite"
        ).parquet(DESTINATION_PATH)

        print("Product data cleaning completed successfully!")

    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
