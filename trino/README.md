# Trino for E-Commerce Data Warehouse

This directory contains Trino configuration and SQL scripts for querying the e-commerce data warehouse.

## Directory Structure

```
trino/
├── catalog/              # Trino catalog configurations
│   ├── .gitkeep
│   └── hive.properties  # Hive Metastore catalog configuration
└── scripts/             # SQL scripts for schema and table management
    ├── create_curated_schema.sql
    └── register_tables.sql
```

## Configuration

### Hive Catalog Configuration (`catalog/hive.properties`)

The `hive.properties` file configures Trino to connect to the Hive Metastore and access data stored in MinIO S3 storage.

Key configuration details:
- **Connector**: Uses Hive connector for metadata management
- **S3 Storage**: Configured for MinIO endpoint with native S3 filesystem
- **Metastore Connection**: Connects to Thrift metastore service
- **Performance Tuning**: Optimized for e-commerce data queries
- **Storage Format**: Uses PARQUET with SNAPPY compression
- **Partitioning**: Automatic partition discovery enabled

**Environment Variables Required:**
- `AWS_ACCESS_KEY_ID`: S3 access key
- `AWS_SECRET_ACCESS_KEY`: S3 secret key

## SQL Scripts

### 1. Create Curated Schema (`scripts/create_curated_schema.sql`)

Creates the main `ecommerce` schema in the Hive catalog for curated data.

```sql
CREATE SCHEMA IF NOT EXISTS hive.ecommerce
WITH (location = 's3a://ecommerce/warehouse/ecommerce.db');
```

### 2. Register Tables (`scripts/register_tables.sql`)

Creates and registers all curated tables in the e-commerce schema. This script includes:

- **Customers**: Customer dimension with SCD Type 2 attributes
- **Products**: Product dimension with comprehensive product attributes
- **Orders**: Fact table with order details and embedded order items array
- **Order Items**: Order item facts with product details
- **Payments**: Payment facts with transaction details and fraud indicators

**Key Features:**
- External tables partitioned by year and month
- PARQUET format with optimized data types
- Automatic partition metadata synchronization
- Comprehensive business attributes for analytics
- Data quality flags for monitoring

**Table Details:**
- All tables are partitioned by time period
- Uses appropriate decimal precision for financial data
- Includes surrogate keys (sk_) for dimension tables
- Contains business logic columns (e.g., profit margins, customer tenure)

## Usage

### Prerequisites
- Trino server running
- Hive Metastore service available
- MinIO S3 storage accessible
- Required environment variables set

### Connecting to the Data
Once configured, you can query the e-commerce data using standard SQL:

```sql
-- Query customer order history
SELECT c.customer_id, c.first_name, c.last_name, 
       o.order_date, o.grand_total, o.order_status
FROM hive.ecommerce.customers c
JOIN hive.ecommerce.orders o ON c.customer_id = o.customer_id
WHERE c.is_current = true AND o.order_date >= '2023-01-01';

-- Analyze product sales by category
SELECT p.category, p.subcategory, 
       COUNT(o.order_id) as order_count,
       SUM(oi.quantity) as total_quantity,
       SUM(oi.line_total) as total_revenue
FROM hive.ecommerce.products p
JOIN hive.ecommerce.order_items oi ON p.product_id = oi.product_id
JOIN hive.ecommerce.orders o ON oi.order_id = o.order_id
GROUP BY p.category, p.subcategory;
```

### Performance Considerations
- Tables are partitioned for efficient query performance
- Use partition pruning in queries by filtering on partition columns
- Prefer column selection over SELECT * for better performance
- Leverage the embedded arrays in orders table for complex joins

## Integration with the Data Pipeline

These Trino configurations and scripts integrate with the broader data pipeline:

1. **Data Generation**: Faker generates raw customer, product, order, and payment data
2. **Data Processing**: Spark cleans and transforms the data into curated format
3. **Data Storage**: Processed data is stored in MinIO S3 in PARQUET format
4. **Data Access**: Trino provides SQL access to the curated data for analytics

## Troubleshooting

If encountering issues:
1. Verify environment variables are set correctly
2. Check that Hive Metastore service is running
3. Ensure MinIO S3 storage is accessible
4. Confirm partition metadata is synchronized correctly
5. Check Trino logs for specific error messages