# Apache Airflow Orchestration for Forge Commerce

This directory contains the Apache Airflow configuration and DAGs for orchestrating the e-commerce data warehouse ETL pipeline.

## Architecture

The Airflow setup uses **CeleryExecutor** for distributed task execution with the following components:

- **PostgreSQL** - Metadata database for Airflow
- **Redis** - Message broker for Celery
- **Airflow Webserver** - Web UI (port 8090)
- **Airflow Scheduler** - Task scheduling
- **Airflow Worker** - Task execution
- **Flower** - Celery monitoring (port 5555, optional profile)

## Directory Structure

```
airflow/
├── Dockerfile              # Airflow Docker image
├── requirements.txt        # Python dependencies
├── README.md              # This file
├── config/                # Airflow configuration files
├── dags/                  # DAG definitions
│   └── ecommerce_etl_dag.py  # Main ETL orchestration DAG
├── logs/                  # Airflow logs (auto-generated)
└── plugins/               # Custom operators and plugins
    ├── __init__.py
    └── spark_operators.py # Custom Spark submit operator
```

## DAGs

### ecommerce_etl_dag

The main ETL orchestration DAG that runs daily at 2:00 AM:

**Task Groups:**
1. **data_generation** - Generate synthetic data using Faker
2. **data_cleaning** - Clean and validate raw data using Spark
3. **data_quality_checks** - Run data quality checks on cleaned data
4. **data_curation** - Build dimension and fact tables using Spark

**Features:**
- Parallel task execution within groups
- Retry logic with exponential backoff
- 4-hour execution timeout
- Email alerts on failure

## Usage

### Starting Airflow

```bash
# Start all services including Airflow
docker compose up -d

# Start with Flower monitoring
docker compose --profile flower up -d
```

### Accessing Airflow

- **Web UI**: http://localhost:8090
- **Username**: airflow
- **Password**: airflow

### Flower Monitoring

- **Flower UI**: http://localhost:5555 (when using --profile flower)

### Running DAGs Manually

1. Open Airflow UI at http://localhost:8090
2. Find "ecommerce_etl_dag" in the DAGs list
3. Click the "Play" button to trigger a manual run

### Pausing/Unpausing DAGs

```bash
# Pause DAG
docker exec airflow-scheduler airflow dags pause ecommerce_etl_dag

# Unpause DAG
docker exec airflow-scheduler airflow dags unpause ecommerce_etl_dag
```

## Custom Operators

### SparkSubmitJobOperator

Custom operator for submitting Spark jobs:

```python
from plugins.spark_operators import SparkSubmitJobOperator

spark_task = SparkSubmitJobOperator(
    task_id='run_spark_job',
    spark_script='/opt/spark/work-dir/src/clean/clean_customers.py',
    spark_master='spark://spark-master:7077',
    driver_memory='2g',
    executor_memory='4g',
    dag=dag,
)
```

## Configuration

Environment variables are loaded from the `.env` file at the project root.

### Key Airflow Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `AIRFLOW_UID` | User ID for Airflow containers | Auto-detected |
| `AIRFLOW_IMAGE_NAME` | Docker image name | `apache/airflow:3.1.8` |
| `_AIRFLOW_WWW_USER_USERNAME` | Admin username | `airflow` |
| `_AIRFLOW_WWW_USER_PASSWORD` | Admin password | `airflow` |

## Monitoring

### Health Checks

- **Webserver**: http://localhost:8090/api/v2/monitor/health
- **Scheduler**: http://localhost:8974/health

### Logs

Logs are stored in `./airflow/logs/` and can be accessed via:

```bash
# View scheduler logs
docker logs airflow-scheduler

# View worker logs
docker logs airflow-worker

# View specific task logs via UI
```

## Troubleshooting

### Common Issues

1. **Permission errors**: Ensure `AIRFLOW_UID` is set correctly
2. **Task timeouts**: Increase `execution_timeout` in DAG default_args
3. **Spark connection failures**: Verify Spark cluster is running
4. **Database connection errors**: Check PostgreSQL container health

### Reset Airflow Database

```bash
# Stop all containers
docker compose down

# Remove Airflow volumes (WARNING: deletes all data)
docker volume rm forge-commerce_postgres-db-volume

# Restart
docker compose up -d
```

## Best Practices

1. **Idempotency**: All tasks should be idempotent (safe to re-run)
2. **Error Handling**: Use try-catch blocks and proper logging
3. **Retries**: Configure appropriate retry policies
4. **Monitoring**: Set up alerts for DAG failures
5. **Documentation**: Document all DAGs and complex tasks