"""
E-Commerce Data Cleaning DAG

This DAG handles only the data cleaning phase of the ETL pipeline,
cleaning raw customer, product, order, and payment data using Spark.

Schedule: Daily at 1:30 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta, datetime
import sys
import os

# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# DAG definition
dag = DAG(
    "ecommerce_data_cleaning_dag",
    default_args=default_args,
    description="E-Commerce Data Cleaning Pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "data-cleaning", "spark"],
    max_active_tasks=1,
)

# Spark configuration common to all Spark jobs
spark_conf = {
    "spark.master": "spark://spark-master:7077",
    "spark.hadoop.fs.s3a.access.key": "forge-commerce-user",
    "spark.hadoop.fs.s3a.secret.key": "forge-commerce-pass",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

# =============================================================================
# Task Definitions
# =============================================================================

# Start task
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

# End task
end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# -----------------------------------------------------------------------------
# Data Cleaning Tasks (using SparkSubmitOperator)
# -----------------------------------------------------------------------------
clean_customers = SparkSubmitOperator(
    task_id="clean_customers",
    application="/opt/spark/work-dir/src/clean/clean_customers.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

clean_products = SparkSubmitOperator(
    task_id="clean_products",
    application="/opt/spark/work-dir/src/clean/clean_products.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

clean_orders = SparkSubmitOperator(
    task_id="clean_orders",
    application="/opt/spark/work-dir/src/clean/clean_orders.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

clean_payments = SparkSubmitOperator(
    task_id="clean_payments",
    application="/opt/spark/work-dir/src/clean/clean_payments.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

# =============================================================================
# Task Dependencies
# =============================================================================

(start >> clean_customers >> clean_products >> clean_orders >> clean_payments >> end)
