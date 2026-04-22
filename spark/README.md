# Spark Processing

This directory contains the Apache Spark processing pipeline for the e-commerce data warehouse. It includes data cleaning, transformation, and curation scripts organized in a structured manner.

## Project Structure

```
spark/
├── .python-version          # Python version specification (3.10)
├── .venv/                   # Virtual environment directory
├── Makefile                 # Makefile for Spark job submission
├── entrypoint.sh           # Docker entrypoint script
├── jupyter.Dockerfile      # Dockerfile for Jupyter environment
├── jupyter_notebook_config.py  # Jupyter notebook configuration
├── notebooks/              # Jupyter notebooks directory
├── poetry.lock             # Poetry dependency lock file
├── pyproject.toml          # Poetry project configuration
├── scripts/                # Utility scripts directory
├── spark.Dockerfile        # Dockerfile for Spark environment
├── src/                    # Source code directory
│   ├── clean/              # Data cleaning scripts
│   │   ├── clean_customers.py
│   │   ├── clean_orders.py
│   │   ├── clean_payments.py
│   │   └── clean_products.py
│   ├── curated/            # Data curation scripts
│   │   ├── curate_customers.py
│   │   ├── curate_order_items.py
│   │   ├── curate_orders.py
│   │   ├── curate_payments.py
│   │   └── curate_products.py
│   └── utils/              # Utility modules
│       ├── __init__.py
│       ├── config.py
│       └── scd2.py
```

## Dependencies

### Core Dependencies
- **Apache Spark**: 3.5.3 (base Docker image)
- **PySpark**: 4.1.1
- **Delta Lake**: 3.2.0 (delta-spark library)
- **Python**: 3.10

### Python Dependencies
- `pandas`: Data manipulation library
- `pyarrow**: Apache Arrow support
- `boto3**: AWS SDK for Python
- `python-dotenv**: Environment variable management

### Additional Jars
- `delta-spark_2.12-3.2.0.jar`
- `delta-storage-3.2.0.jar`
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.530.jar`

## Docker Images

### Production Spark Environment
- **File**: `spark.Dockerfile`
- **Base**: `apache/spark:3.5.3`
- **Purpose**: Production Spark job execution
- **Features**:
  - Java 11 and Python 3.10
  - Delta Lake integration
  - AWS S3 connectivity
  - Pre-installed PySpark and dependencies

### Jupyter Environment
- **File**: `jupyter.Dockerfile`
- **Base**: `apache/spark:3.5.3`
- **Purpose**: Interactive development and analysis
- **Features**:
  - Jupyter Lab installation
  - Same dependencies as production
  - Notebook mounting at `/home/spark/notebooks`

## Configuration

### Environment Variables
The system uses environment variables for configuration. These can be set in a `.env` file or passed to Docker containers.

### Python Configuration
- **File**: `src/utils/config.py`
- **Purpose**: Application configuration management

## Data Processing Pipeline

### Data Cleaning Stage
Located in `src/clean/`, these scripts perform initial data cleaning and validation:

- **clean_customers.py**: Customer data cleaning
- **clean_orders.py**: Order data cleaning
- **clean_payments.py**: Payment data cleaning
- **clean_products.py**: Product data cleaning

### Data Curation Stage
Located in `src/curated/`, these scripts transform cleaned data into warehouse-ready formats:

- **curate_customers.py**: Customer dimension table creation
- **curate_products.py**: Product dimension table creation
- **curate_orders.py**: Order fact table creation
- **curate_order_items.py**: Order items fact table creation
- **curate_payments.py**: Payment fact table creation

### Utilities
- **config.py**: Configuration management
- **scd2.py**: Slowly Changing Dimension Type 2 implementation

## Running Spark Jobs

### Using Make Commands
The `Makefile` provides convenient commands for submitting Spark jobs:

#### Cleaning Jobs
```bash
make spark-submit-clean-customers
make spark-submit-clean-products
make spark-submit-clean-orders
make spark-submit-clean-payments
make spark-submit-clean-all-data
```

#### Curation Jobs
```bash
make spark-submit-curate-customers
make spark-submit-curate-products
make spark-submit-curate-orders
make spark-submit-curate-order-items
make spark-submit-curate-payments
make spark-submit-curate-all-data
```

#### All Jobs
```bash
make spark-submit-all
```

### Direct Spark Submission
Jobs can also be submitted directly using `spark-submit`:

```bash
docker exec spark-submit spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/work-dir/src/clean/clean_customers.py
```

## Jupyter Notebook Interface

### Starting Jupyter
1. Build the Jupyter Docker image:
   ```bash
   docker build -f jupyter.Dockerfile -t spark-jupyter .
   ```

2. Run the container:
   ```bash
   docker run -p 8888:8888 spark-jupyter
   ```

3. Access Jupyter at `http://localhost:8888`

### Configuration
- **File**: `jupyter_notebook_config.py`
- **Token**: No token required (token = '')
- **Password**: No password required
- **Access**: Enabled from all origins (allow_origin = '*')

## Development Workflow

### Local Development
1. Set up Python environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run scripts directly:
   ```bash
   python src/clean/clean_customers.py
   ```

### Docker Development
1. Build the Spark environment:
   ```bash
   docker build -f spark.Dockerfile -t spark-env .
   ```

2. Run interactive sessions:
   ```bash
   docker run -it spark-env /bin/bash
   ```

## Performance Considerations

- Data is processed in batches for memory efficiency
- Delta Lake format provides ACID transactions and time travel
- Partitioning is applied to large datasets for query optimization
- S3A connector enables cloud storage integration

## Error Handling

- Each script includes error handling for data quality issues
- Failed jobs can be re-run from the same checkpoint
- Logging is implemented for debugging purposes

## Integration

The Spark pipeline integrates with:
- **Airflow**: For workflow orchestration
- **Minio**: For object storage (S3-compatible)
- **Trino**: For SQL querying
- **Metastore**: For Hive metadata

## Security

- Docker containers run as non-root users where possible
- Environment variables store sensitive configuration
- IAM roles recommended for production AWS access