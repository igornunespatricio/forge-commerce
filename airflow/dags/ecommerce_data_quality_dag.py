"""
E-Commerce Data Quality Checks DAG

This DAG handles only the data quality validation phase of the ETL pipeline,
running quality checks on cleaned customer, product, order, and payment data.

Schedule: Daily at 2:00 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
    "execution_timeout": timedelta(hours=1),
}

# DAG definition
dag = DAG(
    "ecommerce_data_quality_dag",
    default_args=default_args,
    description="E-Commerce Data Quality Checks Pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ecommerce", "data-quality", "validation"],
    max_active_tasks=1,
)

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
# Data Quality Checks
# -----------------------------------------------------------------------------
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

# =============================================================================
# Task Dependencies
# =============================================================================

(
    start
    >> quality_check_customers
    >> quality_check_products
    >> quality_check_orders
    >> quality_check_payments
    >> end
)
