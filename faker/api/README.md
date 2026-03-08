# E-Commerce Data Generator API

A FastAPI-based REST API for generating synthetic e-commerce data for data warehouse testing and development.

## Overview

This API provides endpoints for generating single records of customers, products, orders, and payments with realistic data patterns and relationships. It reuses the existing batch generation logic from the faker scripts to ensure consistency and performance.

## Features

- **RESTful API Design**: Clean, intuitive endpoints following REST conventions
- **Realistic Data Generation**: Uses proven generation logic with realistic distributions
- **Validation and Error Handling**: Comprehensive validation with detailed error messages
- **Configuration Management**: Environment-based configuration support
- **Logging**: Structured logging for debugging and monitoring
- **OpenAPI Documentation**: Auto-generated API documentation available at `/docs`
- **CORS Support**: Configurable CORS settings for different environments

## Endpoints

### Root
- `GET /` - API information and available endpoints

### Customer Generation
- `GET /api/generate/customer` - Generate a single customer record
  - Optional parameter: `customer_id` - Specific customer ID (1-999,999,999)

### Product Generation
- `GET /api/generate/product` - Generate a single product record
  - Optional parameter: `product_id` - Specific product ID (1-999,999,999)

### Order Generation
- `GET /api/generate/order` - Generate a single order record
  - Optional parameter: `order_id` - Specific order ID (1-999,999,999)
  - Optional parameter: `customer_id` - Specific customer ID (1-999,999,999)

### Payment Generation
- `GET /api/generate/payment` - Generate a single payment record
  - Optional parameter: `payment_id` - Specific payment ID (1-999,999,999)
  - Optional parameter: `order_id` - Specific order ID (1-999,999,999)

### Health and Status
- `GET /api/health` - Health check endpoint
- `GET /api/status` - API status and statistics

## Data Models

### Customer
Generated customers include:
- Personal information (name, email, address, phone)
- Geographic distribution (country, city)
- Customer segment and lifetime value
- Registration and activity dates
- Payment preferences

### Product
Generated products include:
- Product details (name, category, brand, SKU)
- Pricing and cost information
- Inventory levels and supplier details
- Product specifications (weight, dimensions, color, material)
- Quality metrics (rating, review count)

### Order
Generated orders include:
- Order details (status, dates, amounts)
- Customer segment information
- Order items with pricing and discounts
- Shipping and fulfillment details
- Payment method preferences

### Payment
Generated payments include:
- Payment details (method, status, gateway)
- Financial calculations (fees, net amounts)
- Fraud detection scores
- Chargeback information
- Payment metadata (IP, device, browser)

## Installation

1. Install dependencies using Poetry:
```bash
cd faker
poetry install
```

2. Start the API server:
```bash
cd faker/api
poetry run python run.py
```

3. Access the API at `http://localhost:8000`
4. View API documentation at `http://localhost:8000/docs`

### Command Line Options

The `run.py` script supports the following command-line arguments:

```bash
--host HOST           Host to bind to (default: 0.0.0.0)
--port PORT           Port to bind to (default: 8000)
--reload              Enable auto-reload (development mode)
--no-reload           Disable auto-reload (production mode)
--log-level LEVEL     Log level (default: INFO)
```

Examples:
```bash
# Start on a different port
poetry run python run.py --port 9000

# Disable auto-reload for production
poetry run python run.py --no-reload

# Set debug logging
poetry run python run.py --log-level DEBUG
```

## Configuration

The API supports environment-based configuration through the `config.py` file:

### Environment Variables
- `API_HOST` - Server host (default: "0.0.0.0")
- `API_PORT` - Server port (default: "8000")
- `API_RELOAD` - Enable auto-reload (default: "true")
- `ALLOW_ORIGINS` - CORS allowed origins (default: "*")
- `ALLOW_CREDENTIALS` - Allow credentials in CORS requests (default: "true")
- `LOG_LEVEL` - Logging level (default: "INFO")
- `LOG_FORMAT` - Log message format (default: "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
- `ENVIRONMENT` - Environment (development/production/testing)

### Validation Rules
The API enforces the following validation rules:
- Customer ID must be between 1 and 999,999,999
- Product ID must be between 1 and 999,999,999
- Order ID must be between 1 and 999,999,999
- Payment ID must be between 1 and 999,999,999
- Order amount must be greater than 0.01
- Product price must be greater than 0.01
- Product inventory must be greater than or equal to 0

### Configuration Classes
- `DevelopmentConfig` - Development settings with debug logging
- `ProductionConfig` - Production settings with restricted CORS
- `TestingConfig` - Testing settings with minimal logging

## Usage Examples

### Generate a Customer
```bash
curl "http://localhost:8000/api/generate/customer"
```

### Generate a Product with Specific ID
```bash
curl "http://localhost:8000/api/generate/product?product_id=1001"
```

### Generate an Order for a Specific Customer
```bash
curl "http://localhost:8000/api/generate/order?customer_id=12345"
```

### Generate a Payment for a Specific Order
```bash
curl "http://localhost:8000/api/generate/payment?order_id=54321"
```


## Response Format

All endpoints return direct Pydantic model responses with the following structure:

### Customer Response
```json
{
  "customer_id": 12345,
  "customer_uuid": "uuid-string",
  "first_name": "John",
  "last_name": "Doe",
  "email": "john.doe@example.com",
  "phone": "+1-555-0123",
  "date_of_birth": "1985-06-15",
  "registration_date": "2023-01-15",
  "country": "United States",
  "country_code": "US",
  "city": "New York",
  "address": "123 Main St",
  "postal_code": "10001",
  "customer_segment": "Premium",
  "customer_lifetime_value": 1250.50,
  "preferred_payment_method": "Credit Card",
  "is_active": true,
  "last_login_date": "2024-01-15",
  "total_orders": 15,
  "total_spent": 2500.75,
  "created_at": "2024-01-15 10:30:00",
  "updated_at": "2024-01-15 10:30:00"
}
```

### Product Response
```json
{
  "product_id": 1001,
  "product_uuid": "uuid-string",
  "product_name": "Wireless Headphones",
  "sku": "WH-001",
  "upc": "123456789012",
  "category": "Electronics",
  "subcategory": "Audio",
  "brand": "TechBrand",
  "description": "High-quality wireless headphones",
  "price": 99.99,
  "cost_price": 60.00,
  "margin": 39.99,
  "inventory_level": 150,
  "total_value": 14998.50,
  "supplier_name": "Tech Supplier Inc",
  "supplier_country": "China",
  "weight": 0.5,
  "dimensions": "20x15x8",
  "color": "Black",
  "material": "Plastic",
  "product_rating": 4.5,
  "review_count": 120,
  "is_active": true,
  "is_discontinued": false,
  "created_at": "2024-01-01",
  "last_updated": "2024-01-15",
  "warranty_months": 12,
  "return_policy_days": 30
}
```

### Order Response
```json
{
  "order_id": 50001,
  "order_uuid": "uuid-string",
  "customer_id": 12345,
  "customer_segment": "Premium",
  "order_date": "2024-01-15",
  "order_status": "Completed",
  "payment_status": "Paid",
  "payment_method": "Credit Card",
  "shipping_method": "Express",
  "shipping_cost": 9.99,
  "discount_percentage": 10.0,
  "discount_amount": 9.99,
  "tax_rate": 8.0,
  "tax_amount": 7.20,
  "subtotal": 99.99,
  "total_amount": 107.19,
  "fulfillment_date": "2024-01-16",
  "delivery_date": "2024-01-18",
  "tracking_number": "TRK123456789",
  "delivery_address": "123 Main St",
  "delivery_city": "New York",
  "delivery_country": "United States",
  "delivery_postal_code": "10001",
  "order_items": [
    {
      "product_id": 1001,
      "product_name": "Wireless Headphones",
      "category": "Electronics",
      "subcategory": "Audio",
      "brand": "TechBrand",
      "unit_price": 99.99,
      "quantity": 1,
      "discount_percentage": 10.0,
      "line_total": 89.99
    }
  ],
  "created_at": "2024-01-15 10:30:00",
  "updated_at": "2024-01-18 14:45:00"
}
```

### Payment Response
```json
{
  "payment_id": 30001,
  "payment_uuid": "uuid-string",
  "order_id": 50001,
  "customer_id": 12345,
  "customer_segment": "Premium",
  "payment_method": "Credit Card",
  "payment_status": "Completed",
  "payment_gateway": "Stripe",
  "payment_date": "2024-01-15",
  "payment_time": "10:30:15",
  "currency_code": "USD",
  "order_amount": 107.19,
  "transaction_fee_rate": 2.9,
  "transaction_fee": 3.11,
  "net_amount": 104.08,
  "fraud_score": 0.15,
  "chargeback_amount": null,
  "chargeback_date": null,
  "chargeback_reason": null,
  "payment_reference": "PAY-123456789",
  "payment_metadata": {
    "ip_address": "192.168.1.1",
    "user_agent": "Mozilla/5.0...",
    "device_type": "Desktop",
    "browser": "Chrome"
  },
  "created_at": "2024-01-15 10:30:15",
  "updated_at": "2024-01-15 10:30:15"
}
```

## Error Handling

The API provides detailed error responses:

```json
{
  "error": "Validation error",
  "message": "Invalid request parameters",
  "details": [
    {
      "loc": ["query", "customer_id"],
      "msg": "ensure this value is greater than 0",
      "type": "value_error.number.not_gt"
    }
  ]
}
```

## Logging

The API includes comprehensive logging features:

### Request/Response Logging
All incoming requests and outgoing responses are automatically logged with:
- Request method and URL
- Response status codes
- Timestamp information

### Log Output
Logs are written to:
- Console output (stdout)
- `api.log` file in the API directory

### Log Levels
Available log levels (configurable via `LOG_LEVEL` environment variable):
- `DEBUG` - Detailed debugging information
- `INFO` - General information (default)
- `WARNING` - Warning messages
- `ERROR` - Error messages
- `CRITICAL` - Critical error messages

### Log Format
The default log format includes:
- Timestamp
- Logger name
- Log level
- Message content

Custom format can be configured via the `LOG_FORMAT` environment variable.

## Development

### Adding New Endpoints
1. Add the endpoint function to `app.py`
2. Import required models and generators
3. Add proper validation and error handling
4. Update the OpenAPI documentation with docstrings

### Modifying Data Generation
1. Update the corresponding generator class in the faker scripts
2. The API will automatically use the updated logic
3. Run tests to ensure compatibility

### Configuration Changes
1. Modify the appropriate config class in `config.py`
2. Add new environment variables if needed
3. Update documentation

## Dependencies

- **FastAPI** - Web framework
- **uvicorn** - ASGI server
- **pandas** - Data processing
- **numpy** - Numerical operations
- **faker** - Data generation
- **pydantic** - Data validation

## License

This project follows the same license as the main forge-commerce repository.

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation
4. Ensure backward compatibility
5. Run the test suite before submitting changes