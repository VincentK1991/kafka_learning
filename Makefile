.PHONY: help install install-dev setup start-kafka start-producer start-consumer start-api-producer start-api-consumer stop-kafka status clean format lint type-check test build-images start-all stop-all monitoring start-extraction stop-extraction

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install project dependencies
	uv venv --python 3.12
	source .venv/bin/activate && uv sync

install-dev: ## Install project with development dependencies
	uv venv --python 3.12
	source .venv/bin/activate && uv sync --extra dev

setup: ## Complete project setup (install deps + start kafka + setup db)
	chmod +x start_pipeline.sh
	./start_pipeline.sh

start-kafka: ## Start Kafka infrastructure
	docker compose -f docker-compose.infrastructure.yml up -d

start-extraction: ## Start 5 extraction worker instances
	@echo "Starting 5 extraction workers..."
	@mkdir -p logs
	@for i in 1 2 3 4 5; do \
		echo "Starting extraction worker $$i"; \
		source .venv/bin/activate && nohup python -m indexing_pipeline.extraction.extraction_api > logs/extraction_worker_$$i.log 2>&1 & \
		echo $$! > logs/extraction_worker_$$i.pid; \
	done
	@echo "✅ All 5 extraction workers started. Check logs/ directory for output."

stop-extraction: ## Stop all extraction worker instances
	@echo "Stopping extraction workers..."
	@if [ -d logs ]; then \
		for pidfile in logs/extraction_worker_*.pid; do \
			if [ -f "$$pidfile" ]; then \
				pid=$$(cat "$$pidfile"); \
				if kill -0 "$$pid" 2>/dev/null; then \
					echo "Stopping worker with PID $$pid"; \
					kill "$$pid"; \
				fi; \
				rm -f "$$pidfile"; \
			fi; \
		done; \
	fi
	@echo "✅ All extraction workers stopped."


