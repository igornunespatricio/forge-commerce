# E-Commerce Data Generation Suite

This directory contains a comprehensive data generation system for creating synthetic e-commerce data that follows the project's `.clinerules` for data generation best practices.

## Overview

The data generation suite creates realistic synthetic data for:
- **Customers** (5M-10M records)
- **Products** (100K-500K records) 
- **Orders** (20M-50M records)
- **Payments** (20M-50M records)

All generated data follows realistic business patterns, seasonal trends, and data quality standards.

## Data Generation Scripts

### 1. Customer Data Generation (`generate_customers.py`)

Generates synthetic customer data with realistic demographics, geographic distribution, and behavioral patterns.

**Key Features:**
- Realistic customer segments (premium, regular, occasional)
- Geographic distribution across countries and cities
- Age and income distributions
- Customer lifetime value patterns
- Preferred payment methods
- Data quality validation

**Usage:**
```bash
python generate_customers.py --total-records 1000000 --batch-size 50000 --output-dir data/raw/customers
```

### 2. Product Data Generation (`generate_products.py`)

Generates synthetic product catalog data with realistic categories, pricing, and inventory patterns.

**Key Features:**
- Multiple product categories and subcategories
- Realistic pricing distributions
- Brand distributions
- Inventory level management
- Product lifecycle states
- Data quality validation

**Usage:**
```bash
python generate_products.py --total-records 200000 --batch-size 25000 --output-dir data/raw/products
```

### 3. Order Data Generation (`generate_orders.py`)

Generates synthetic order data with realistic customer behavior patterns, seasonal trends, and order fulfillment workflows.

**Key Features:**
- Customer behavior patterns by segment
- Seasonal shopping trends
- Product category preferences
- Order status distributions
- Shipping method preferences
- Discount and pricing logic
- Order fulfillment workflows
- Data quality validation

**Usage:**
```bash
python generate_orders.py --total-records 5000000 --batch-size 100000 --output-dir data/raw/orders --customer-data data/raw/customers --product-data data/raw/products
```

### 4. Payment Data Generation (`generate_payments.py`)

Generates synthetic payment data with realistic payment patterns, fraud detection scenarios, and payment method distributions.

**Key Features:**
- Payment method distributions by customer segment
- Payment status patterns
- Fraud score generation
- Transaction fee calculations
- Chargeback scenarios
- Payment gateway distributions
- Currency distributions
- Data quality validation

**Usage:**
```bash
python generate_payments.py --total-records 5000000 --batch-size 100000 --output-dir data/raw/payments --order-data data/raw/orders
```

## Common Features

### Batch Processing
All scripts support batch processing to handle large data volumes efficiently:
- Configurable batch sizes
- Progress tracking and logging
- Memory-efficient processing
- Atomic file writes

### Multiple Output Formats
All scripts support multiple output formats:
- **CSV** (default) - Comma-separated values
- **JSON** - JSON format with ISO date formatting
- **Parquet** - Columnar format for big data processing

### Data Quality Validation
Each script includes comprehensive data validation:
- Required field validation
- Data type validation
- Business rule validation
- Referential integrity checks
- Range validation for numeric fields

### Configuration Options
Common configuration options across all scripts:
- `--total-records` - Total number of records to generate
- `--batch-size` - Number of records per batch
- `--output-dir` - Output directory for generated files
- `--output-format` - Output file format (csv, json, parquet)
- `--start-id` - Starting ID for generated records
- `--seed` - Random seed for reproducible results

### Logging and Monitoring
- Comprehensive logging to both file and console
- Progress tracking with percentage completion
- Error handling and detailed error messages
- Performance metrics and timing information

## Data Quality Standards

### Referential Integrity
- Customer IDs in orders reference valid customers
- Product IDs in orders reference valid products
- Order IDs in payments reference valid orders
- Customer segments are consistent across related data

### Business Logic Validation
- Order dates are after customer creation dates
- Payment dates are after order dates
- Order amounts are positive
- Product prices are positive
- Inventory levels are non-negative

### Data Distribution Standards
- Realistic demographic distributions
- Seasonal shopping patterns
- Customer behavior patterns by segment
- Payment method preferences
- Geographic distributions

## Make File Usage

The `Makefile` provides convenient commands for running the data generation scripts:

### Available Commands

```bash
# Generate customer data (1000 rows, batch size 100, JSON output)
make customers

# Generate product data
make products

# Generate order data
make orders

# Generate payment data
make payments

# Generate all data types
make all

# Install dependencies with Poetry
make install

# Clean generated data files
make clean

# View available commands
make help
```

### Default Parameters

The Makefile uses the following default parameters:

- **Customers**: 1000 records, batch size 100, JSON output
- **Products**: 1000 records, batch size 100, JSON output  
- **Orders**: 5000 records, batch size 500, JSON output
- **Payments**: 5000 records, batch size 500, JSON output

### Data Directory

All generated data is stored in `../data/raw/` subdirectories:
- Customers: `../data/raw/customers/`
- Products: `../data/raw/products/`
- Orders: `../data/raw/orders/`
- Payments: `../data/raw/payments/`

## Dependencies

### Required Python Packages
- `pandas` - Data manipulation and file I/O
- `faker` - Synthetic data generation
- `numpy` - Numerical operations
- `python-dateutil` - Date manipulation

### Optional Dependencies
- `pyarrow` - Parquet file support
- `fastparquet` - Alternative Parquet implementation

Install dependencies:
```bash
poetry install