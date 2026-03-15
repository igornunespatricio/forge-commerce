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

spark-submit-clean-customers:
	@echo "Submitting Spark job: Clean Customers"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/clean/clean_customers.py

spark-submit-clean-products:
	@echo "Submitting Spark job: Clean Products"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/clean/clean_products.py

spark-submit-clean-orders:
	@echo "Submitting Spark job: Clean Orders"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/clean/clean_orders.py

spark-submit-clean-payments:
	@echo "Submitting Spark job: Clean Payments"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/clean/clean_payments.py

spark-submit-clean-all-data:
	@echo "Submitting Spark job: Clean All"
	make spark-submit-clean-customers
	make spark-submit-clean-products
	make spark-submit-clean-orders
	make spark-submit-clean-payments
