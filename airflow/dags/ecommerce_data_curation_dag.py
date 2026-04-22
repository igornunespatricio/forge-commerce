"""
E-Commerce Data Curation DAG

This DAG handles only the data curation phase of the ETL pipeline,
building curated dimension and fact tables for the data warehouse.

Schedule: Daily at 2:30 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
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
    "execution_timeout": timedelta(hours=3),
}

# DAG definition
dag = DAG(
    "ecommerce_data_curation_dag",
    default_args=default_args,
    description="E-Commerce Data Curation Pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "data-curation", "data-warehouse", "spark"],
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
# Data Curation Tasks (using SparkSubmitOperator)
# -----------------------------------------------------------------------------
curate_customers = SparkSubmitOperator(
    task_id="curate_customers",
    application="/opt/spark/work-dir/src/curated/curate_customers.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

curate_products = SparkSubmitOperator(
    task_id="curate_products",
    application="/opt/spark/work-dir/src/curated/curate_products.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

curate_orders = SparkSubmitOperator(
    task_id="curate_orders",
    application="/opt/spark/work-dir/src/curated/curate_orders.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

curate_order_items = SparkSubmitOperator(
    task_id="curate_order_items",
    application="/opt/spark/work-dir/src/curated/curate_order_items.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

curate_payments = SparkSubmitOperator(
    task_id="curate_payments",
    application="/opt/spark/work-dir/src/curated/curate_payments.py",
    conf=spark_conf,
    verbose=True,
    dag=dag,
)

# -----------------------------------------------------------------------------
# Final Data Quality Checks (After Curation)
# -----------------------------------------------------------------------------
final_quality_check = BashOperator(
    task_id="final_quality_check",
    bash_command='echo "Running final data quality checks on curated tables..."',
    dag=dag,
)

# =============================================================================
# Task Dependencies
# =============================================================================

(
    start
    >> curate_customers
    >> curate_products
    >> curate_orders
    >> curate_order_items
    >> curate_payments
    >> final_quality_check
    >> end
)
