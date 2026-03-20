up:
	@echo "Starting containers"
	docker compose up -d

down:
	@echo "Stopping containers"
	docker compose down

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