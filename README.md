# Forge Commerce - E-Commerce Data Warehouse at Scale

A comprehensive e-commerce data warehouse platform built with modern data engineering technologies, designed to handle large-scale synthetic data generation, processing, and analytics for testing and development purposes.

## Overview

Forge Commerce is a complete data platform that simulates a real-world e-commerce environment with customer data, products, orders, and payments. The platform uses cutting-edge technologies to generate, process, and analyze data at scale, making it ideal for testing data pipelines, analytics systems, and machine learning models.

## Architecture

The platform consists of several interconnected components:

### Core Components

1. **Data Generation** - Faker-based synthetic data generation
2. **Data Processing** - Apache Spark for data transformation
3. **Data Storage** - MinIO S3-compatible object storage
4. **Orchestration** - Apache Airflow for workflow management
5. **Analytics** - Trino for SQL queries and analysis
6. **Metadata** - Hive Metastore for table management

### Data Flow

```
Faker Data Generation → MinIO Raw Storage → Spark Processing → 
Curated Storage → Trino Analytics
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- At least 8GB RAM (16GB recommended)
- 2+ CPU cores

### Setup

1. Clone the repository:
```bash
git clone https://github.com/igornunespatricio/forge-commerce.git
cd forge-commerce
```

2. Create environment file:
```bash
cp .env.example .env
```

3. Start all services:
```bash
docker-compose up -d
```

4. Verify services are running:
```bash
docker-compose ps
```

### Accessing Services

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Trino UI**: http://localhost:8088
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Spark UI**: http://localhost:8080
- **Jupyter Lab**: http://localhost:8888 (development only)

## Platform Components

### 1. Faker Data Generator (`faker/`)

Generates realistic e-commerce synthetic data:

- **Customers**: 5M-10M records with demographics and behavior patterns
- **Products**: 100K-1M records with categories and pricing
- **Orders**: 20M-50M records with temporal patterns
- **Payments**: 20M-50M records with transaction details

**Key Features**:
- Batch processing with configurable sizes
- REST API for individual record generation
- Realistic data distributions and patterns
- Data validation and quality checks

**Usage**:
```bash
# Navigate to faker directory
cd faker

# Generate customer data
make customers

# Generate all data types
make all

# Start API server
make api
```

### 2. Spark Processing (`spark/`)

Processes raw data into curated warehouse format:

- **Data Cleaning**: Validation and standardization
- **Data Curation**: Building dimension and fact tables
- **Delta Lake**: ACID transactions and time travel support
- **Partitioning**: Optimized for analytical queries

**Key Features**:
- Docker containerized environment
- Jupyter Lab for interactive development
- Configurable batch processing
- S3 storage integration

**Usage**:
```bash
# Navigate to spark directory
cd spark

# Clean customer data
make spark-submit-clean-customers

# Curate all data
make spark-submit-curate-all-data

# Start Jupyter (development)
make jupyter
```

### 3. Airflow Orchestration (`airflow/`)

Manages end-to-end ETL workflows:

- **DAGs**: Separate DAGs for each pipeline stage
- **Scheduling**: Daily automated execution
- **Monitoring**: Task execution tracking
- **Dependencies**: Service health checks

**Key Features**:
- Modular DAG design
- Celery executor for distributed processing
- PostgreSQL metadata database
- Redis message broker

**Usage**:
```bash
# Access Airflow UI
open http://localhost:8080

# Trigger manual DAG runs
# Use Airflow UI for DAG management
```

### 4. Trino Analytics (`trino/`)

Provides SQL access to curated data:

- **Hive Catalog**: Connects to Hive Metastore
- **Partition Pruning**: Optimized query performance
- **S3 Storage**: Direct access to curated data
- **SQL Interface**: ANSI SQL compatibility

**Key Features**:
- External table support
- Columnar format optimization
- Schema discovery
- Query performance monitoring

**Usage**:
```bash
# Connect to Trino
curl -H "X-Trino-User: admin" http://localhost:8088

# Sample queries
# SELECT * FROM hive.ecommerce.customers LIMIT 10;
# SELECT category, SUM(total_amount) FROM hive.ecommerce.orders GROUP BY category;
```

### 5. MinIO Storage (`minio/`)

S3-compatible object storage:

- **Raw Data**: Initial data generation output
- **Curated Data**: Processed warehouse tables
- **Configuration**: Environment-based access
- **Console**: Web interface for management

**Key Features**:
- S3 API compatibility
- Data lifecycle management
- Encryption support
- Network isolation

## Data Pipeline

### 1. Data Generation

```mermaid
graph TD
    A[Faker Scripts] --> B[Customer Data]
    A --> C[Product Data]
    A --> D[Order Data]
    A --> E[Payment Data]
    B --> F[MinIO Raw Storage]
    C --> F
    D --> F
    E --> F
```

### 2. Data Cleaning

```mermaid
graph TD
    G[Spark Cleaning Jobs] --> H[Customer Validation]
    G --> I[Product Validation]
    G --> J[Order Validation]
    G --> K[Payment Validation]
    H --> L[Standardized Format]
    I --> L
    J --> L
    K --> L
    L --> M[MinIO Clean Storage]
```

### 3. Data Curation

```mermaid
graph TD
    N[Spark Curation Jobs] --> O[Dimension Tables]
    N --> P[Fact Tables]
    O --> Q[Delta Lake Storage]
    P --> Q
    Q --> R[Trino Analytics]
```

## Configuration

### Environment Variables

Key variables in `.env` file:

```env
# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_S3_ENDPOINT=http://localhost:9000

# Airflow Configuration
AIRFLOW__CORE__EXECUTOR=CeleryExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0

# Spark Configuration
SPARK_MASTER=spark://spark-master:7077
AWS_S3_PATH_STYLE_ACCESS=true
```

### Docker Compose Profiles

- **Default**: Core services (MinIO, Spark, Airflow, Trino)
- **Development**: Includes Jupyter Lab
- **Scaling**: Additional Spark workers
- **Flower**: Celery monitoring

## Monitoring and Logging

### Log Locations

- **Airflow**: `airflow/logs/`
- **Spark**: `spark/logs/`
- **MinIO**: `minio_data/`
- **Trino**: Container logs

### Health Checks

All services implement health checks:

```bash
# Check service status
docker-compose ps

# View logs
docker-compose logs -f [service-name]
```

## Development

### Adding New Components

1. Create new directory for component
2. Add Docker configuration
3. Update docker-compose.yml
4. Document in README
5. Add to data pipeline if needed

### Testing

```bash
# Test data generation
cd faker && make test

# Test Spark jobs
cd spark && make test

# Test SQL queries
trino --execute "SELECT 1"
```

## Performance Considerations

### Resource Requirements

- **Minimum**: 8GB RAM, 2 CPU cores
- **Recommended**: 16GB RAM, 4 CPU cores
- **Production**: 32GB RAM, 8+ CPU cores

### Optimization Tips

1. Adjust Docker resource limits in docker-compose.yml
2. Configure Spark executor memory based on data volume
3. Use appropriate partitioning for large datasets
4. Monitor query performance with Trino UI

## Troubleshooting

### Common Issues

1. **Service Not Starting**:
   - Check logs with `docker-compose logs [service]`
   - Verify environment variables
   - Ensure sufficient resources

2. **Data Generation Failures**:
   - Check MinIO connectivity
   - Verify bucket permissions
   - Review faker logs

3. **Spark Job Failures**:
   - Check Spark UI for errors
   - Verify S3 configuration
   - Monitor memory usage

4. **Airflow DAG Issues**:
   - Check database connectivity
   - Verify DAG file syntax
   - Review task logs

### Debug Mode

Enable debug logging:

```bash
# Airflow
export AIRFLOW__CORE__LOG_LEVEL=DEBUG

# Spark
export SPARK_CONF="spark.executor.extraJavaOptions=-Dlog4j.debug=true"
```

## Contributing

1. Fork the repository
2. Create feature branch
3. Follow existing patterns
4. Update documentation
5. Test changes thoroughly
6. Submit pull request

## License

This project follows the MIT License. See LICENSE file for details.

## Support

For issues and questions:
1. Check troubleshooting section
2. Review service logs
3. Consult component documentation
4. Create GitHub issue if needed

## Future Enhancements

- Machine learning model training pipelines
- Real-time data streaming with Kafka
- Enhanced monitoring with Grafana
- Multi-tenant data isolation
- Automated data quality reporting
``