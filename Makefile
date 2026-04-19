up:
	@echo "Starting containers"
	docker compose up -d

platform-up:
	@echo "Starting platform services: Spark, Airflow, MinIO"
	@echo "Excluding: metastore-db, metastore, trino"
	docker compose up -d minio minio-init spark-master spark-worker-1 spark-submit postgres redis airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer

down:
	@echo "Stopping containers"
	docker compose down

down-clean:
	@echo "Stopping containers and removing all volumes"
	docker compose down --volumes

build:
	@echo "Building all images"
	docker compose build

all-containers: down build up

logs:
	@echo "Showing container logs"
	docker compose logs -f

generate-faker-data:
	@echo "Generating Faker data"
	cd faker && make all

spark-submit-clean-all-data:
	@echo "Submitting Spark job: Clean All"
	cd spark && make spark-submit-clean-all-data

spark-submit-curate-all-data:
	@echo "Submitting Spark job: Curate All"
	cd spark && make spark-submit-curate-all-data

spark-submit-all:
	@echo "Submitting All Spark jobs"
	cd spark && make spark-submit-all

# =============================================================================
# Data Lake Commands
# =============================================================================

data-lake-up:
	@echo "Starting data lake services (MinIO, MariaDB, Hive Metastore, Trino)"
	docker compose down
	docker compose up -d minio minio-init metastore-db metastore trino

data-lake-down:
	@echo "Stopping data lake services"
	docker compose down

data-lake-logs:
	@echo "Showing data lake logs"
	docker compose logs -f minio minio-init metastore-db metastore trino

data-lake-status:
	@echo "Checking data lake service status"
	docker compose ps minio minio-init metastore-db metastore trino

data-lake-reset:
	@echo "Resetting data lake services (WARNING: deletes all metastore data)"
	docker compose down
	docker volume rm forge-commerce_metastore-db-volume
	@echo "Metastore database reset complete. You can now run 'make data-lake-up' again."

data-lake-rebuild:
	@echo "Rebuilding metastore image with no cache (complete clean build)"
	docker compose build --no-cache metastore
	@echo "Metastore image rebuilt successfully."

# =============================================================================
# Airflow Commands
# =============================================================================

airflow-up:
	@echo "Starting Airflow services"
	docker compose up -d postgres redis airflow-init airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer

airflow-up-with-flower:
	@echo "Starting Airflow services with Flower monitoring"
	docker compose --profile flower up -d

airflow-down:
	@echo "Stopping Airflow services"
	docker compose down

airflow-logs:
	@echo "Showing Airflow logs"
	docker compose logs -f airflow-scheduler airflow-worker

airflow-status:
	@echo "Checking Airflow service status"
	docker compose ps

airflow-webserver:
	@echo "Opening Airflow web UI"
	@echo "Access Airflow at: http://localhost:8090"
	@echo "Username: airflow"
	@echo "Password: airflow"

airflow-flower:
	@echo "Opening Flower monitoring UI"
	@echo "Access Flower at: http://localhost:5555"

airflow-reset:
	@echo "Resetting Airflow database (WARNING: deletes all data)"
	docker compose down
	docker volume rm forge-commerce_postgres-db-volume
	docker compose up -d postgres redis airflow-init
