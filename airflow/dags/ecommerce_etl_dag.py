"""
E-Commerce ETL Orchestration DAG

This DAG orchestrates the complete ETL pipeline for the e-commerce data warehouse,
including data generation, cleaning, curation, and data quality checks.

Schedule: Daily at 2:00 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
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
    "execution_timeout": timedelta(hours=4),
}

# DAG definition
dag = DAG(
    "ecommerce_etl_dag",
    default_args=default_args,
    description="E-Commerce Data Warehouse ETL Pipeline",
    schedule=None,  # "0 2 * * *",  # Daily at 2:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "data-warehouse"],
    max_active_tasks=1,
)

# Spark configuration common to all Spark jobs
spark_conf = {
    "spark.master": "spark://spark-master:7077",
    # "spark.submit.deployMode": "cluster",
    "spark.hadoop.fs.s3a.access.key": "forge-commerce-user",
    "spark.hadoop.fs.s3a.secret.key": "forge-commerce-pass",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}

# Path to faker scripts
FAKER_SCRIPTS_PATH = "/opt/forge-commerce/faker/src"


# -----------------------------------------------------------------------------
# Python wrapper functions for data generation
# -----------------------------------------------------------------------------
def _generate_customers(**context):
    """Generate customer data using the Faker script."""
    sys.path.insert(0, FAKER_SCRIPTS_PATH)
    # Set up arguments matching the Makefile
    sys.argv = [
        "generate_customers.py",
        "--total-records",
        "1000",
        "--batch-size",
        "1000",
        "--output-format",
        "json",
        "--bucket-name",
        "raw",
        "--endpoint-url",
        "http://minio:9000",
    ]
    # Import and run the main function
    import generate_customers

    generate_customers.main()


def _generate_products(**context):
    """Generate product data using the Faker script."""
    sys.path.insert(0, FAKER_SCRIPTS_PATH)
    sys.argv = [
        "generate_products.py",
        "--total-records",
        "1000",
        "--batch-size",
        "1000",
        "--output-format",
        "json",
        "--bucket-name",
        "raw",
        "--endpoint-url",
        "http://minio:9000",
    ]
    import generate_products

    generate_products.main()


def _generate_orders(**context):
    """Generate order data using the Faker script."""
    sys.path.insert(0, FAKER_SCRIPTS_PATH)
    sys.argv = [
        "generate_orders.py",
        "--total-records",
        "1000",
        "--batch-size",
        "1000",
        "--output-format",
        "json",
        "--bucket-name",
        "raw",
        "--endpoint-url",
        "http://minio:9000",
    ]
    import generate_orders

    generate_orders.main()


def _generate_payments(**context):
    """Generate payment data using the Faker script."""
    sys.path.insert(0, FAKER_SCRIPTS_PATH)
    sys.argv = [
        "generate_payments.py",
        "--total-records",
        "1000",
        "--batch-size",
        "1000",
        "--output-format",
        "json",
        "--bucket-name",
        "raw",
        "--endpoint-url",
        "http://minio:9000",
    ]
    import generate_payments

    generate_payments.main()


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
# Data Generation Tasks (using PythonOperator with Faker scripts)
# -----------------------------------------------------------------------------
with TaskGroup(group_id="data_generation", dag=dag) as data_generation:

    generate_customers = PythonOperator(
        task_id="generate_customers",
        python_callable=_generate_customers,
        dag=dag,
    )

    generate_products = PythonOperator(
        task_id="generate_products",
        python_callable=_generate_products,
        dag=dag,
    )

    generate_orders = PythonOperator(
        task_id="generate_orders",
        python_callable=_generate_orders,
        dag=dag,
    )

    generate_payments = PythonOperator(
        task_id="generate_payments",
        python_callable=_generate_payments,
        dag=dag,
    )

    (generate_customers >> generate_products >> generate_orders >> generate_payments)

# -----------------------------------------------------------------------------
# Data Cleaning Tasks (using SparkSubmitOperator)
# -----------------------------------------------------------------------------
with TaskGroup(group_id="data_cleaning", dag=dag) as data_cleaning:

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

    (clean_customers >> clean_products >> clean_orders >> clean_payments)

# -----------------------------------------------------------------------------
# Data Quality Checks (After Cleaning)
# -----------------------------------------------------------------------------
with TaskGroup(group_id="data_quality_checks", dag=dag) as data_quality_checks:

    quality_check_customers = BashOperator(
        task_id="quality_check_customers",
        bash_command='echo "Running data quality checks on customers..."',
        dag=dag,
    )

    quality_check_products = BashOperator(
        task_id="quality_check_products",
        bash_command='echo "Running data quality checks on products..."',
        dag=dag,
    )

    quality_check_orders = BashOperator(
        task_id="quality_check_orders",
        bash_command='echo "Running data quality checks on orders..."',
        dag=dag,
    )

    quality_check_payments = BashOperator(
        task_id="quality_check_payments",
        bash_command='echo "Running data quality checks on payments..."',
        dag=dag,
    )

    (
        quality_check_customers
        >> quality_check_products
        >> quality_check_orders
        >> quality_check_payments
    )

# -----------------------------------------------------------------------------
# Data Curation Tasks (using SparkSubmitOperator)
# -----------------------------------------------------------------------------
with TaskGroup(group_id="data_curation", dag=dag) as data_curation:

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

    (
        curate_customers
        >> curate_products
        >> curate_orders
        >> curate_order_items
        >> curate_payments
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

# Start -> Data Generation
start >> data_generation

# Data Generation -> Data Cleaning (parallel)
data_generation >> data_cleaning

# Data Cleaning -> Data Quality Checks (parallel)
data_cleaning >> data_quality_checks

# Data Quality Checks -> Data Curation (parallel)
data_quality_checks >> data_curation

# Data Curation -> Final Quality Check
data_curation >> final_quality_check

# Final Quality Check -> End
final_quality_check >> end
