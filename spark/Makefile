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


spark-submit-curate-customers:
	@echo "Submitting Spark job: Curate Customers"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/curated/curate_customers.py


spark-submit-curate-products:
	@echo "Submitting Spark job: Curate Products"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/curated/curate_products.py


spark-submit-curate-orders:
	@echo "Submitting Spark job: Curate Orders"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/curated/curate_orders.py

spark-submit-curate-order-items:
	@echo "Submitting Spark job: Curate Order Items"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/curated/curate_order_items.py

spark-submit-curate-payments:
	@echo "Submitting Spark job: Curate Payments"
	docker exec spark-submit spark-submit \
		--master spark://spark-master:7077 \
		/opt/spark/work-dir/src/curated/curate_payments.py

spark-submit-curate-all-data: 
	@echo "Submitting Spark job: Curate All"
	make spark-submit-curate-customers
	make spark-submit-curate-products
	make spark-submit-curate-orders
	make spark-submit-curate-order-items
# 	make spark-submit-curate-payments

spark-submit-all:
	@echo "Submitting All Spark jobs"
	make spark-submit-clean-all-data
	make spark-submit-curate-all-data