"""
E-Commerce Data Generation DAG

This DAG handles only the data generation phase of the ETL pipeline,
generating customer, product, order, and payment data using Faker.

Schedule: Daily at 1:00 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
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
    "ecommerce_data_generation_dag",
    default_args=default_args,
    description="E-Commerce Data Generation Pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "data-generation", "faker"],
    max_active_tasks=1,
)

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
        "ecommerce",
        "--filepath-prefix",
        "raw/customers",
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
        "ecommerce",
        "--filepath-prefix",
        "raw/products",
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
        "ecommerce",
        "--filepath-prefix",
        "raw/orders",
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
        "ecommerce",
        "--filepath-prefix",
        "raw/payments",
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
# Data Generation Tasks
# -----------------------------------------------------------------------------
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

# =============================================================================
# Task Dependencies
# =============================================================================

(
    start
    >> generate_customers
    >> generate_products
    >> generate_orders
    >> generate_payments
    >> end
)
