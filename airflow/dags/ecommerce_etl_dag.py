"""
E-Commerce ETL Orchestration DAG

This DAG orchestrates the complete ETL pipeline by triggering individual
specialized DAGs in sequence. All actual task logic lives in separate DAG files.

Schedule: Daily at 2:00 AM
Owner: Data Engineering Team
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
# Trigger individual specialized DAGs
# -----------------------------------------------------------------------------
trigger_data_generation = TriggerDagRunOperator(
    task_id="trigger_data_generation",
    trigger_dag_id="ecommerce_data_generation_dag",
    wait_for_completion=True,
    reset_dag_run=True,
    poke_interval=30,
    dag=dag,
)

trigger_data_cleaning = TriggerDagRunOperator(
    task_id="trigger_data_cleaning",
    trigger_dag_id="ecommerce_data_cleaning_dag",
    wait_for_completion=True,
    reset_dag_run=True,
    poke_interval=30,
    dag=dag,
)

trigger_data_quality = TriggerDagRunOperator(
    task_id="trigger_data_quality",
    trigger_dag_id="ecommerce_data_quality_dag",
    wait_for_completion=True,
    reset_dag_run=True,
    poke_interval=30,
    dag=dag,
)

trigger_data_curation = TriggerDagRunOperator(
    task_id="trigger_data_curation",
    trigger_dag_id="ecommerce_data_curation_dag",
    wait_for_completion=True,
    reset_dag_run=True,
    poke_interval=30,
    dag=dag,
)

# =============================================================================
# Task Dependencies
# =============================================================================

(
    start
    >> trigger_data_generation
    >> trigger_data_cleaning
    >> trigger_data_quality
    >> trigger_data_curation
    >> end
)
