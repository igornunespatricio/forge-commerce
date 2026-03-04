# Makefile for Faker Data Generation

.PHONY: help customers products orders payments clean install

# Configuration variables
DATA_DIR = ../data/raw

# Default target
help:
	@echo "Available targets:"
	@echo "  customers  - Generate customer data (1000 rows, batch size 100, JSON output)"
	@echo "  products   - Generate product data"
	@echo "  orders     - Generate order data"
	@echo "  payments   - Generate payment data"
	@echo "  all        - Generate all data types"
	@echo "  install    - Install dependencies with Poetry"
	@echo "  clean      - Clean generated data files"

# Generate customer data with specified parameters
customers:
	@echo "Generating customer data..."
	@poetry run python generate_customers.py \
		--total-records 1000 \
		--batch-size 100 \
		--output-dir $(DATA_DIR)/customers \
		--output-format json

# Generate product data (placeholder - to be implemented)
products:
	@echo "Generating product data..."
	@poetry run python generate_products.py \
		--total-records 10000 \
		--batch-size 1000 \
		--output-dir $(DATA_DIR)/products \
		--output-format json

# Generate order data (placeholder - to be implemented)
orders:
	@echo "Generating order data..."
	@poetry run python generate_orders.py \
		--total-records 5000 \
		--batch-size 500 \
		--output-dir $(DATA_DIR)/orders \
		--output-format json

# Generate payment data (placeholder - to be implemented)
payments:
	@echo "Generating payment data..."
	@poetry run python generate_payments.py \
		--total-records 5000 \
		--batch-size 500 \
		--output-dir $(DATA_DIR)/payments \
		--output-format json

# Generate all data types
all: customers products orders payments
	@echo "All data generation completed!"

# Install dependencies
install:
	@echo "Installing dependencies with Poetry..."
	@poetry install

# Clean generated data files
clean:
	@echo "Cleaning generated data files..."
	@rm -rf $(DATA_DIR)/customers/*.csv $(DATA_DIR)/customers/*.json $(DATA_DIR)/customers/*.parquet
	@rm -rf $(DATA_DIR)/products/*.csv $(DATA_DIR)/products/*.json $(DATA_DIR)/products/*.parquet
	@rm -rf $(DATA_DIR)/orders/*.csv $(DATA_DIR)/orders/*.json $(DATA_DIR)/orders/*.parquet
	@rm -rf $(DATA_DIR)/payments/*.csv $(DATA_DIR)/payments/*.json $(DATA_DIR)/payments/*.parquet
	@echo "Clean completed!"
