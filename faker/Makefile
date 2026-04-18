# Makefile for Faker Data Generation

.PHONY: help customers products orders payments clean install api

# Configuration variables
SCRIPT_DIR = src

# Default target
help:
	@echo "Available targets:"
	@echo "  customers  - Generate customer data (1M rows, batch size 50K, JSON output)"
	@echo "  products   - Generate product data"
	@echo "  orders     - Generate order data"
	@echo "  payments   - Generate payment data"
	@echo "  all        - Generate all data types"
	@echo "  install    - Install dependencies with Poetry"
	@echo "  clean      - Clean generated data files"

# Generate customer data with specified parameters
customers:
	@echo "Generating customer data..."
	@poetry run python $(SCRIPT_DIR)/generate_customers.py \
		--total-records 1000 \
		--batch-size 1000 \
		--output-format json \
		--bucket-name ecommerce \
		--filepath-prefix raw/customers

# Generate product data (placeholder - to be implemented)
products:
	@echo "Generating product data..."
	@poetry run python $(SCRIPT_DIR)/generate_products.py \
		--total-records 1000 \
		--batch-size 1000 \
		--output-format json \
		--bucket-name ecommerce \
		--filepath-prefix raw/products

# Generate order data (placeholder - to be implemented)
orders:
	@echo "Generating order data..."
	@poetry run python $(SCRIPT_DIR)/generate_orders.py \
		--total-records 1000 \
		--batch-size 1000 \
		--output-format json \
		--bucket-name ecommerce \
		--filepath-prefix raw/orders

# Generate payment data (placeholder - to be implemented)
payments:
	@echo "Generating payment data..."
	@poetry run python $(SCRIPT_DIR)/generate_payments.py \
		--total-records 1000 \
		--batch-size 1000 \
		--output-format json \
		--bucket-name ecommerce \
		--filepath-prefix raw/payments

# Generate all data types
all: customers products orders payments
	@echo "All data generation completed!"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	@poetry install

#Serve API
api:
	@echo "Starting API server..."
	@poetry run python api/run.py