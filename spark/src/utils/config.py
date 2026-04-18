import os

ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "forge-commerce-user")
SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "forge-commerce-pass")
S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "http://minio:9000")

BUCKET = "ecommerce"

RAW_PATH_CUSTOMERS = f"s3a://{BUCKET}/raw/customers/"
CLEAN_PATH_CUSTOMERS = f"s3a://{BUCKET}/clean/customers/"
CURATED_PATH_CUSTOMERS = f"s3a://{BUCKET}/curated/customers/"

RAW_PATH_PRODUCTS = f"s3a://{BUCKET}/raw/products/"
CLEAN_PATH_PRODUCTS = f"s3a://{BUCKET}/clean/products/"
CURATED_PATH_PRODUCTS = f"s3a://{BUCKET}/curated/products/"

RAW_PATH_ORDERS = f"s3a://{BUCKET}/raw/orders/"
CLEAN_PATH_ORDERS = f"s3a://{BUCKET}/clean/orders/"
CURATED_PATH_ORDERS = f"s3a://{BUCKET}/curated/orders/"

RAW_PATH_PAYMENTS = f"s3a://{BUCKET}/raw/payments/"
CLEAN_PATH_PAYMENTS = f"s3a://{BUCKET}/clean/payments/"
CURATED_PATH_PAYMENTS = f"s3a://{BUCKET}/curated/payments/"

CURATED_PATH_ORDER_ITEMS = f"s3a://{BUCKET}/curated/order_items/"
