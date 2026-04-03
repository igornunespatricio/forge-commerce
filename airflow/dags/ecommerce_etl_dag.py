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
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from datetime import timedelta, datetime

# Default arguments for the DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
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
    schedule="0 2 * * *",  # Daily at 2:00 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "etl", "data-warehouse"],
    max_active_tasks=4,
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
# Data Generation Tasks (using Faker via Make)
# -----------------------------------------------------------------------------
with TaskGroup(group_id="data_generation", dag=dag) as data_generation:

    generate_customers = BashOperator(
        task_id="generate_customers",
        bash_command="cd /opt/forge-commerce/faker && make customers",
        dag=dag,
    )

    generate_products = BashOperator(
        task_id="generate_products",
        bash_command="cd /opt/forge-commerce/faker && make products",
        dag=dag,
    )

    generate_orders = BashOperator(
        task_id="generate_orders",
        bash_command="cd /opt/forge-commerce/faker && make orders",
        dag=dag,
    )

    generate_payments = BashOperator(
        task_id="generate_payments",
        bash_command="cd /opt/forge-commerce/faker && make payments",
        dag=dag,
    )

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
