up:
	@echo "Starting containers"
	docker compose up -d

down:
	@echo "Stopping containers"
	docker compose down

rebuild:
	@echo "Rebuilding all images"
	docker compose build

rebuild-all:
	@echo "Stopping containers, rebuilding all images, and starting again"
	docker compose down
	docker compose build
	docker compose up -d

logs:
	@echo "Showing container logs"
	docker compose logs -f