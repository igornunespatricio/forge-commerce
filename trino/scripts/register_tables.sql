-- Clean up existing tables to refresh schema with correct decimal types
DROP TABLE IF EXISTS hive.ecommerce.customers;
DROP TABLE IF EXISTS hive.ecommerce.products;
DROP TABLE IF EXISTS hive.ecommerce.orders;
DROP TABLE IF EXISTS hive.ecommerce.order_items;
DROP TABLE IF EXISTS hive.ecommerce.payments;

CREATE TABLE IF NOT EXISTS hive.ecommerce.customers (
    sk_customer BIGINT,
    customer_id INTEGER,
    first_name VARCHAR,
    last_name VARCHAR,
    name VARCHAR,
    email VARCHAR,
    email_domain VARCHAR,
    phone VARCHAR,
    phone_clean VARCHAR,
    address VARCHAR,
    full_address VARCHAR,
    city VARCHAR,
    country VARCHAR,
    country_code VARCHAR,
    date_of_birth DATE,
    customer_age INTEGER,
    registration_date DATE,
    customer_tenure_days INTEGER,
    last_login_date DATE,
    days_since_last_login INTEGER,
    total_orders INTEGER,
    total_spent DECIMAL(12,2),
    avg_spent_per_order DECIMAL(10,2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    days_since_update INTEGER,
    inactive_customer BOOLEAN,
    row_hash VARCHAR,
    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_active BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    creation_year INTEGER,
    creation_month INTEGER
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://ecommerce/curated/customers'
);

CREATE TABLE IF NOT EXISTS  hive.ecommerce.products (
    sk_product BIGINT,
    product_id INTEGER,
    product_name VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    brand VARCHAR,
    description VARCHAR,
    supplier_name VARCHAR,
    supplier_country VARCHAR,
    color VARCHAR,
    material VARCHAR,
    price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    margin DECIMAL(8,4),
    weight DECIMAL(8,3),
    dimensions VARCHAR,
    inventory_level INTEGER,
    product_rating DECIMAL(3,2),
    review_count INTEGER,
    is_discontinued BOOLEAN,
    is_active BOOLEAN,
    created_at TIMESTAMP,
    last_updated TIMESTAMP,
    creation_year INTEGER,
    creation_month INTEGER,
    full_category VARCHAR,
    profit_per_unit DECIMAL(10,2),
    total_profit_potential DECIMAL(14,2),
    product_age_days INTEGER,
    days_since_update INTEGER,
    price_category VARCHAR,
    weight_category VARCHAR,
    length_cm DECIMAL(8,2),
    width_cm DECIMAL(8,2),
    height_cm DECIMAL(8,2),
    volume_cm3 DECIMAL(12,3),
    dimensions_ratio DECIMAL(8,4),
    low_stock_flag BOOLEAN,
    old_product_flag BOOLEAN,
    low_rating_flag BOOLEAN,
    status_inconsistency_flag BOOLEAN,
    invalid_dimensions_flag BOOLEAN,
    row_hash VARCHAR,
    is_current BOOLEAN,
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://ecommerce/curated/products'
);

CREATE TABLE IF NOT EXISTS hive.ecommerce.orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    order_status VARCHAR,
    total_amount DECIMAL(12,2),
    shipping_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    discount_amount DECIMAL(12,2),
    grand_total DECIMAL(12,2),
    shipping_method VARCHAR,
    payment_method VARCHAR,
    shipping_address VARCHAR,
    billing_address VARCHAR,
    shipping_city VARCHAR,
    shipping_country VARCHAR,
    order_items ARRAY(ROW(
        product_id VARCHAR,
        product_name VARCHAR,
        category VARCHAR,
        subcategory VARCHAR,
        brand VARCHAR,
        unit_price DECIMAL(10,2),
        quantity INTEGER,
        discount_percentage DECIMAL(5,2),
        line_total DECIMAL(12,2)
    )),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    order_year INTEGER,
    order_month INTEGER,
    sk_customer BIGINT
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://ecommerce/curated/orders'
);

CREATE TABLE IF NOT EXISTS hive.ecommerce.order_items (
    order_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    sk_product BIGINT,
    product_name VARCHAR,
    category VARCHAR,
    subcategory VARCHAR,
    brand VARCHAR,
    unit_price DECIMAL(10,2),
    quantity INTEGER,
    discount_percentage DECIMAL(5,2),
    line_total DECIMAL(12,2),
    created_at TIMESTAMP,
    order_date DATE,
    order_year INTEGER,
    order_month INTEGER
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://ecommerce/curated/order_items'
);

CREATE TABLE IF NOT EXISTS hive.ecommerce.payments (
    payment_id INTEGER,
    order_id INTEGER,
    customer_id INTEGER,
    order_amount DECIMAL(10,2),
    net_amount DECIMAL(12,2),
    transaction_fee DECIMAL(10,2),
    transaction_fee_rate DECIMAL(6,4),
    chargeback_amount DECIMAL(12,2),
    payment_method VARCHAR,
    payment_gateway VARCHAR,
    payment_status VARCHAR,
    currency_code VARCHAR,
    payment_reference VARCHAR,
    payment_uuid VARCHAR,
    payment_date DATE,
    payment_time VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    payment_timestamp TIMESTAMP,
    chargeback_date DATE,
    chargeback_reason VARCHAR,
    fraud_score DECIMAL(5,2),
    fraud_risk_level VARCHAR,
    customer_segment VARCHAR,
    browser VARCHAR,
    device_type VARCHAR,
    ip_address VARCHAR,
    user_agent VARCHAR,
    is_successful_payment BOOLEAN,
    is_failed_payment BOOLEAN,
    is_pending_payment BOOLEAN,
    has_chargeback BOOLEAN,
    missing_payment_date_flag BOOLEAN,
    missing_chargeback_date_flag BOOLEAN,
    long_chargeback_time_flag BOOLEAN,
    negative_net_amount_flag BOOLEAN,
    status_inconsistency_flag BOOLEAN,
    payment_year INTEGER,
    payment_month INTEGER,
    payment_hour INTEGER,
    payment_day_of_week INTEGER,
    sk_customer BIGINT
)
WITH (
  format = 'PARQUET',
  external_location = 's3a://ecommerce/curated/payments'
);