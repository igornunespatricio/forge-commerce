up:
	@echo "Starting containers"
	docker compose up -d

down:
	@echo "Stopping containers"
	docker compose down

rebuild:
	@echo "Rebuilding all images"
	docker compose build

all:
	@echo "Stopping containers, rebuilding all images, and starting again"
	docker compose down
	docker compose build
	docker compose up -d

logs:
	@echo "Showing container logs"
	docker compose logs -f

spark-submit:
	@echo "Submitting Spark job"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/clean_customers.py
