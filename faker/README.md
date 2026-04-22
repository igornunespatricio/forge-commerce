# Forge Commerce Faker Data Generator

A comprehensive synthetic data generation system for e-commerce data warehouse testing and development. This module generates realistic customer, product, order, and payment data with proper relationships, patterns, and business rules.

## Overview

The Faker module provides both batch generation scripts and a REST API for creating synthetic e-commerce data. It follows industry best practices for data generation, including realistic distributions, temporal patterns, data validation, and proper relationships between entities.

## Features

- **Batch Generation**: Scripts for generating large volumes of customer, product, order, and payment data
- **REST API**: FastAPI-based endpoints for generating individual records
- **Realistic Data**: Follows e-commerce patterns with seasonal trends, customer segments, and realistic distributions
- **Data Validation**: Comprehensive validation rules and quality checks
- **Scalable Processing**: Batch processing with configurable sizes for memory efficiency
- **Output Formats**: Support for JSON output with extensibility for CSV and Parquet
- **Storage Integration**: Direct integration with MinIO for data storage
- **API Documentation**: Auto-generated OpenAPI documentation

## Project Structure

```
faker/
├── src/                    # Data generation scripts
│   ├── generate_customers.py   # Customer data generation
│   ├── generate_products.py    # Product data generation
│   ├── generate_orders.py      # Order data generation
│   └── generate_payments.py   # Payment data generation
├── api/                    # REST API implementation
│   ├── app.py              # FastAPI application
│   ├── config.py           # Configuration management
│   ├── models.py           # Pydantic models
│   ├── run.py              # API entry point
│   └── README.md           # API documentation
├── Makefile               # Command automation
├── pyproject.toml        # Project dependencies and configuration
├── .venv/               # Virtual environment
├── logs/                # Log files
└── README.md           # This file
```

## Data Generation Targets

| Entity | Target Volume | Key Features |
|--------|---------------|--------------|
| **Customers** | 5M-10M records | Geographic distribution, customer segments, lifetime value, registration patterns |
| **Products** | 100K-1M records | Categories, pricing, inventory levels, suppliers, quality metrics |
| **Orders** | 20M-50M records | Customer behavior, seasonal trends, order status, fulfillment workflows |
| **Payments** | 20M-50M records | Payment methods, fraud detection, transaction fees, chargebacks |

## Installation

### Prerequisites

- Python 3.9+
- Poetry (for dependency management)
- MinIO instance or compatible S3 storage

### Setup

1. Navigate to the faker directory:
```bash
cd faker
```

2. Install dependencies:
```bash
poetry install
```

3. Create necessary directories:
```bash
mkdir -p logs
```

## Usage

### Command Line Interface

The Makefile provides convenient commands for data generation:

```bash
# Generate customer data (1M records, batch size 50K, JSON output)
make customers

# Generate product data
make products

# Generate order data
make orders

# Generate payment data
make payments

# Generate all data types
make all

# Install dependencies
make install

# Start API server
make api
```

### Direct Script Execution

Each script can be run directly with various parameters:

```bash
# Generate customer data with custom parameters
poetry run python src/generate_customers.py \
    --total-records 1000000 \
    --batch-size 50000 \
    --output-format json \
    --start-date 2020-01-01 \
    --end-date 2024-12-31 \
    --bucket-name forge-commerce \
    --endpoint-url http://localhost:9000 \
    --filepath-prefix raw/customers
```

### API Usage

1. Start the API server:
```bash
cd api
poetry run python run.py
```

2. Access the API at `http://localhost:8000`
3. View API documentation at `http://localhost:8000/docs`

#### API Endpoints

- `GET /api/generate/customer` - Generate a single customer record
- `GET /api/generate/product` - Generate a single product record
- `GET /api/generate/order` - Generate a single order record
- `GET /api/generate/payment` - Generate a single payment record
- `GET /api/health` - Health check endpoint

#### API Examples

```bash
# Generate a customer
curl "http://localhost:8000/api/generate/customer"

# Generate a product with specific ID
curl "http://localhost:8000/api/generate/product?product_id=1001"

# Generate an order for a specific customer
curl "http://localhost:8000/api/generate/order?customer_id=12345"

# Generate a payment for a specific order
curl "http://localhost:8000/api/generate/payment?order_id=54321"
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BUCKET_NAME` | `forge-commerce` | MinIO bucket name |
| `ENDPOINT_URL` | `http://localhost:9000` | MinIO endpoint URL |
| `AWS_ACCESS_KEY_ID` | `admin` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | `password` | AWS secret key |
| `FILEPATH_PREFIX` | `raw` | Base path for generated files |

### Script Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--total-records` | 1,000,000 | Total number of records to generate |
| `--batch-size` | 50,000 | Number of records per batch |
| `--output-format` | `json` | Output format (json, csv, parquet) |
| `--start-date` | `2020-01-01` | Start date for date-based generation |
| `--end-date` | `2024-12-31` | End date for date-based generation |
| `--start-id` | 1 | Starting ID for generated records |
| `--seed` | 42 | Random seed for reproducible results |
| `--filepath-prefix` | entity name | File path prefix for storage |

## Data Models

### Customer Data

Generated customers include:
- Personal information (name, email, address, phone)
- Geographic distribution (country, city, postal code)
- Customer segmentation (premium, regular, occasional)
- Lifetime value and spending patterns
- Registration and activity dates
- Payment method preferences

### Product Data

Generated products include:
- Product details (name, category, brand, SKU, UPC)
- Pricing information (price, cost, margin)
- Inventory levels and supplier details
- Product specifications (weight, dimensions, color, material)
- Quality metrics (rating, review count)
- Status and warranty information

### Order Data

Generated orders include:
- Order details (status, dates, tracking numbers)
- Customer and segment information
- Order items with pricing and discounts
- Shipping and fulfillment details
- Tax and payment calculations
- Delivery address information

### Payment Data

Generated payments include:
- Payment details (method, status, gateway, reference)
- Financial calculations (fees, net amounts)
- Fraud detection scores
- Chargeback information
- Payment metadata (IP, device, browser)
- Transaction timestamps

## Data Quality and Validation

### Validation Rules

Each script implements comprehensive validation:
- Required field checks
- Data type validation
- Range validation (prices, quantities, dates)
- Business rule enforcement
- Relationship integrity checks

### Data Generation Patterns

The system implements realistic data patterns:
- Seasonal trends in customer registrations and orders
- Customer lifetime value distributions
- Product category preferences by customer segment
- Payment method distributions by region
- Order status transitions and workflows
- Fraud score distributions

### Batch Processing

Large datasets are processed in configurable batches:
- Memory-efficient generation
- Progress tracking and logging
- Error handling and recovery
- Atomic file uploads to storage

## API Configuration

The REST API provides flexible configuration options:

### Command Line Arguments

```bash
poetry run python run.py [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--host` | `0.0.0.0` | Host to bind to |
| `--port` | `8000` | Port to bind to |
| `--reload` | `true` | Enable auto-reload (development) |
| `--no-reload` | | Disable auto-reload (production) |
| `--log-level` | `INFO` | Logging level |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `API_HOST` | `0.0.0.0` | Server host |
| `API_PORT` | `8000` | Server port |
| `API_RELOAD` | `true` | Auto-reload enabled |
| `ALLOW_ORIGINS` | `*` | CORS allowed origins |
| `LOG_LEVEL` | `INFO` | Logging level |
| `ENVIRONMENT` | `development` | Environment setting |

## Integration with Data Warehouse

The generated data follows data warehouse best practices:

### Star Schema Design

- Fact tables: `fact_orders`, `fact_payments`
- Dimension tables: `dim_customer`, `dim_product`, `dim_date`, `dim_location`

### Data Partitioning

- Daily partitions for fact tables
- Strategic partitioning keys (date, region, product category)
- Partition pruning support for query optimization

### Data Quality

- Referential integrity between tables
- Data validation at generation time
- Proper handling of null values
- Data lineage tracking

## Performance Considerations

### Batch Processing

- Configurable batch sizes for memory management
- Progress tracking for long-running operations
- Parallel processing capabilities
- Checkpointing for fault tolerance

### Storage Optimization

- Efficient file formats (JSON, Parquet)
- Proper file naming conventions
- Atomic file uploads
- Compression support

### API Performance

- FastAPI for high-performance endpoints
- Response caching for repeated requests
- Proper error handling and logging
- Monitoring and metrics support

## Development

### Adding New Data Types

1. Create a new generation script in `src/`
2. Implement the generator class with appropriate methods
3. Add command line argument parsing
4. Implement validation logic
5. Update the Makefile with new targets
6. Add API endpoints if needed

### Modifying Existing Generators

1. Update the appropriate generation script
2. Ensure backward compatibility
3. Test with existing data pipelines
4. Update documentation
5. Consider performance implications

## Dependencies

### Core Dependencies

- **faker** - Data generation library
- **pandas** - Data manipulation and analysis
- **numpy** - Numerical computing
- **fastapi** - Web framework for API
- **uvicorn** - ASGI server
- **pydantic** - Data validation

### Development Dependencies

- **pytest** - Testing framework
- **black** - Code formatting
- **flake8** - Code linting
- **mypy** - Type checking

## Logging and Monitoring

### Log Files

- `logs/generate_customers.log` - Customer generation logs
- `logs/generate_products.log` - Product generation logs
- `logs/generate_orders.log` - Order generation logs
- `logs/generate_payments.log` - Payment generation logs
- `logs/api.log` - API request/response logs

### Log Levels

- `DEBUG` - Detailed debugging information
- `INFO` - General information and progress
- `WARNING` - Warning messages
- `ERROR` - Error messages
- `CRITICAL` - Critical errors

## Troubleshooting

### Common Issues

1. **Memory Issues**: Reduce batch size in script parameters
2. **Storage Errors**: Verify MinIO connection and bucket access
3. **API Connection**: Check port configuration and firewall settings
4. **Data Validation**: Review validation rules and business logic

### Debug Mode

Enable debug logging for troubleshooting:

```bash
# For scripts
poetry run python src/generate_customers.py --log-level DEBUG

# For API
poetry run python run.py --log-level DEBUG
```

## Contributing

1. Follow the existing code style and patterns
2. Add appropriate tests for new functionality
3. Update documentation for changes
4. Ensure backward compatibility
5. Run the test suite before submitting changes

## License

This project follows the same license as the main forge-commerce repository.

## Support

For issues and questions:
1. Check the troubleshooting section
2. Review log files for error details
3. Consult the API documentation
4. Contact the data engineering team

---

This Faker module provides a comprehensive solution for generating synthetic e-commerce data with realistic patterns, proper relationships, and business rules. It supports both batch generation for large datasets and API access for individual record generation, making it suitable for various data warehouse testing and development scenarios.