# Airflow Orchestration for Forge Commerce Data Pipeline

This directory contains the Apache Airflow configuration and DAGs for orchestrating the e-commerce data pipeline. Airflow manages the end-to-end ETL workflow, from data generation through to data curation and quality checks.

## Directory Structure

```
airflow/
├── Dockerfile                  # Docker configuration for Airflow container
├── entrypoint.sh              # Entry script for Airflow container
├── requirements.txt           # Python dependencies for Airflow
├── config/                    # Airflow configuration files
│   ├── airflow.cfg           # Main Airflow configuration
│   └── .gitkeep              # Placeholder for additional config files
├── dags/                     # DAG definitions
│   ├── ecommerce_etl_dag.py              # Main ETL orchestration DAG
│   ├── ecommerce_data_generation_dag.py  # Data generation DAG
│   ├── ecommerce_data_cleaning_dag.py    # Data cleaning DAG
│   ├── ecommerce_data_quality_dag.py    # Data quality checks DAG
│   └── ecommerce_data_curation_dag.py   # Data curation DAG
├── logs/                     # Log files (mounted volume)
├── plugins/                  # Custom Airflow plugins
│   ├── __init__.py          # Plugin initialization
│   └── spark_operators.py    # Custom Spark operators
└── README.md                # This file
```

## Overview

The Airflow setup for Forge Commerce implements a modular ETL pipeline with the following key components:

1. **DAG Orchestration**: Each ETL phase is managed by a separate DAG, allowing for independent execution and troubleshooting.
2. **Containerization**: The Airflow service is containerized using Docker with Java support for Spark operations.
3. **Plugin System**: Custom operators are provided for specialized tasks, particularly for Spark job submissions.
4. **Configuration Management**: Separate configuration files for environment-specific settings.

## DAGs

### Main ETL Orchestration DAG (`ecommerce_etl_dag.py`)

This DAG coordinates the entire ETL pipeline by triggering specialized DAGs in sequence:

- **Trigger**: Data Generation
- **Trigger**: Data Cleaning
- **Trigger**: Data Quality Checks
- **Trigger**: Data Curation

**Key Features**:
- Uses `TriggerDagRunOperator` for modular DAG management
- Waits for completion of each downstream DAG
- Scheduled for daily execution at 2:00 AM
- Tags: `["ecommerce", "etl", "data-warehouse"]`

### Data Generation DAG (`ecommerce_data_generation_dag.py`)

Generates synthetic e-commerce data using Faker:

- **Tasks**:
  - `generate_customers`: Creates customer data
  - `generate_products`: Creates product data
  - `generate_orders`: Creates order data
  - `generate_payments`: Creates payment data

**Data Output**:
- Format: JSON
- Location: MinIO S3 bucket (`ecommerce` bucket, `raw/` prefix)
- Batch size: 1,000 records per task

**Key Features**:
- Uses Python operators to execute Faker scripts
- Scheduled for daily execution at 1:00 AM
- Tags: `["ecommerce", "data-generation", "faker"]`

### Data Cleaning DAG (`ecommerce_data_cleaning_dag.py`)

Cleans raw data using Apache Spark:

- **Tasks**:
  - `clean_customers`: Validates and standardizes customer data
  - `clean_products`: Validates and standardizes product data
  - `clean_orders`: Validates and standardizes order data
  - `clean_payments`: Validates and standardizes payment data

**Spark Configuration**:
- Master: `spark://spark-master:7077`
- Storage: S3-compatible (MinIO)
- Delta Lake extensions enabled
- Execution timeout: 2 hours

**Key Features**:
- Uses `SparkSubmitOperator` for each cleaning task
- Scheduled for daily execution at 1:30 AM
- Tags: `["ecommerce", "data-cleaning", "spark"]`

### Data Quality Checks DAG (`ecommerce_data_quality_dag.py`)

Validates data quality across cleaned datasets:

- **Tasks**:
  - `quality_check_customers`: Runs customer data validation
  - `quality_check_products`: Runs product data validation
  - `quality_check_orders`: Runs order data validation
  - `quality_check_payments`: Runs payment data validation

**Key Features**:
- Uses Bash operators for validation scripts
- Scheduled for daily execution at 2:00 AM
- Tags: `["ecommerce", "data-quality", "validation"]`

### Data Curation DAG (`ecommerce_data_curation_dag.py`)

Builds curated dimension and fact tables for the data warehouse:

- **Tasks**:
  - `curate_customers`: Builds customer dimension table
  - `curate_products`: Builds product dimension table
  - `curate_orders`: Builds orders table
  - `curate_order_items`: Builds order items table
  - `curate_payments`: Builds payment table
  - `final_quality_check`: Runs final validation on curated tables

**Key Features**:
- Uses `SparkSubmitOperator` for curation tasks
- Builds Delta Lake tables for analytical querying
- Scheduled for daily execution at 2:30 AM
- Tags: `["ecommerce", "data-curation", "data-warehouse", "spark"]`

## Configuration

### Airflow Configuration (`airflow.cfg`)

Key settings include:

- **Executor**: LocalExecutor for standalone operation
- **Database**: SQLite for metadata storage
- **Parallelism**: 32 concurrent tasks
- **Timezone**: UTC
- **Load Examples**: Enabled for development
- **Authentication**: Simple auth manager with admin user

### Docker Configuration (`Dockerfile`)

The Docker image includes:

- Base: Apache Airflow 3.1.8
- Java 17 for Spark operations
- Python dependencies from `requirements.txt`
- PySpark and Delta Lake support
- Hadoop AWS connector for S3 integration

### Environment Variables

Key environment variables for configuration:

- `AIRFLOW_HOME`: Path to Airflow home directory
- `AIRFLOW__CORE__DAGS_FOLDER`: Path to DAGs directory
- `AIRFLOW__CORE__LOAD_EXAMPLES`: Enable/disable DAG examples
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: Database connection string

## Custom Plugins

### Spark Operators (`plugins/spark_operators.py`)

Custom `SparkSubmitJobOperator` for submitting Spark jobs:

**Features**:
- Builds spark-submit commands with configurable options
- Supports Spark configuration properties
- Handles JARs and Python files
- Implements timeout handling
- Provides detailed error logging

**Configuration Options**:
- `spark_script`: Path to Python script
- `spark_master`: Spark master URL
- `conf`: Spark configuration properties
- `jars`: Additional JAR files
- `py_files`: Additional Python files
- `driver_memory`: Driver memory setting
- `executor_memory`: Executor memory setting
- `num_executors`: Number of executors
- `app_name`: Application name

## Setup and Installation

### Prerequisites

- Docker and Docker Compose
- S3-compatible storage (MinIO)
- Spark cluster
- PostgreSQL database (optional, for production)

### Building the Docker Image

```bash
docker build -t forge-commerce-airflow -f airflow/Dockerfile .
```

### Running with Docker Compose

The Forge Commerce project includes a `docker-compose.yml` file that sets up the complete environment:

```bash
docker-compose up -d airflow
```

### Accessing the Airflow UI

Once running, access the Airflow web UI at:

```
http://localhost:8080
```

Login with:
- Username: `admin`
- Password: `admin`

## Monitoring and Logging

### Log Files

- **Location**: `/opt/airflow/logs`
- **Task Logs**: Organized by DAG ID, run ID, and task ID
- **Log Level**: INFO (configurable in `airflow.cfg`)

### Monitoring DAG Runs

1. Access the Airflow UI
2. Navigate to the DAGs page
3. Click on a DAG to view runs
4. Monitor task execution and logs

### Error Handling

- Each DAG implements retry logic with exponential backoff
- Tasks have timeout settings to prevent hanging
- Failed tasks trigger email notifications (if configured)
- Detailed error messages are logged for troubleshooting

## Development and Testing

### Adding New DAGs

1. Create a new Python file in the `dags/` directory
2. Follow the existing DAG structure and patterns
3. Include appropriate documentation and tags
4. Test in a development environment before deploying

### Modifying DAGs

- DAGs are automatically reloaded when modified
- Changes are reflected in the UI after a short delay
- Test DAG changes using the "Play" button in the UI

### Testing DAGs

1. Pause the DAG in the UI if production
2. Use the "Trigger" or "Play" button for test runs
3. Monitor task execution and logs
4. Validate output data and processing results

## Troubleshooting

### Common Issues

1. **DAG Not Showing**:
   - Check file syntax and DAG definition
   - Verify file permissions
   - Check Airflow logs for parsing errors

2. **Task Failures**:
   - Review task logs for detailed error messages
   - Verify connectivity to dependent services (Spark, MinIO)
   - Check resource availability (memory, CPU)

3. **Connection Issues**:
   - Verify connection configuration in Airflow UI
   - Check network connectivity between services
   - Validate credentials and permissions

### Debugging Tips

- Increase log level to DEBUG for detailed information
- Use the Airflow UI's "Graph" view to visualize dependencies
- Check the Airflow scheduler and worker logs
- Monitor resource usage with system tools

## Scaling and Performance

### Horizontal Scaling

For production environments, consider:

- Using CeleryExecutor with multiple workers
- Implementing database clustering
- Adding load balancing for multiple Airflow instances

### Performance Optimization

- Tune task parallelism based on available resources
- Optimize Spark configurations for cluster size
- Implement proper caching and data partitioning
- Monitor and adjust timeout settings based on job complexity

## Security Considerations

- Use strong passwords for authentication
- Implement proper access controls
- Store sensitive information in Airflow variables or connections
- Regularly update dependencies to address security vulnerabilities
- Use HTTPS for web UI access in production

## Future Enhancements

Potential improvements include:

- Implementing DAG dependencies across environments
- Adding data quality metrics and reporting
- Incorporating machine learning model training pipelines
- Enhancing monitoring with custom metrics
- Implementing automated performance tuning