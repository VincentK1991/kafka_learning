.PHONY: help install install-dev setup start-kafka start-producer start-consumer start-api-producer start-api-consumer stop-kafka status clean format lint type-check test build-images start-all stop-all monitoring start-extraction stop-extraction start-pipeline stop-pipeline

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
	@bash -c '. .venv/bin/activate && METRICS_PORT=8002 nohup python -m indexing_pipeline.extraction.extraction_api > logs/extraction_worker_1.log 2>&1 & echo $$! > logs/extraction_worker_1.pid'
	@echo "Starting extraction worker 1"
	@bash -c '. .venv/bin/activate && METRICS_PORT=8003 nohup python -m indexing_pipeline.extraction.extraction_api > logs/extraction_worker_2.log 2>&1 & echo $$! > logs/extraction_worker_2.pid'
	@echo "Starting extraction worker 2"
	@bash -c '. .venv/bin/activate && METRICS_PORT=8004 nohup python -m indexing_pipeline.extraction.extraction_api > logs/extraction_worker_3.log 2>&1 & echo $$! > logs/extraction_worker_3.pid'
	@echo "Starting extraction worker 3"
	@bash -c '. .venv/bin/activate && METRICS_PORT=8005 nohup python -m indexing_pipeline.extraction.extraction_api > logs/extraction_worker_4.log 2>&1 & echo $$! > logs/extraction_worker_4.pid'
	@echo "Starting extraction worker 4"
	@bash -c '. .venv/bin/activate && METRICS_PORT=8006 nohup python -m indexing_pipeline.extraction.extraction_api > logs/extraction_worker_5.log 2>&1 & echo $$! > logs/extraction_worker_5.pid'
	@echo "Starting extraction worker 5"
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

start-pipeline: ## Start the complete pipeline (ingestion + extraction + normalization)
	@echo "Starting pipeline services..."
	@mkdir -p logs
	@echo "Starting text ingestion API..."
	@nohup ./start_ingestion.sh > logs/ingestion_api.log 2>&1 & echo $$! > logs/ingestion_api.pid
	@echo "Starting extraction API..."
	@nohup ./start_extraction.sh > logs/extraction_api.log 2>&1 & echo $$! > logs/extraction_api.pid
	@echo "Starting normalization API..."
	@nohup ./start_normalization.sh > logs/normalization_api.log 2>&1 & echo $$! > logs/normalization_api.pid
	@echo "✅ Pipeline started successfully!"
	@echo "  - Text Ingestion API: logs/ingestion_api.log"
	@echo "  - Extraction API: logs/extraction_api.log"
	@echo "  - Normalization API: logs/normalization_api.log"

stop-pipeline: ## Stop all pipeline services
	@echo "Stopping pipeline services..."
	@if [ -d logs ]; then \
		echo "Stopping text ingestion API..."; \
		if [ -f logs/ingestion_api.pid ]; then \
			pid=$$(cat logs/ingestion_api.pid); \
			if kill -0 "$$pid" 2>/dev/null; then \
				kill "$$pid" && echo "Stopped ingestion API (PID $$pid)"; \
			fi; \
			rm -f logs/ingestion_api.pid; \
		fi; \
		echo "Stopping extraction API..."; \
		if [ -f logs/extraction_api.pid ]; then \
			pid=$$(cat logs/extraction_api.pid); \
			if kill -0 "$$pid" 2>/dev/null; then \
				kill "$$pid" && echo "Stopped extraction API (PID $$pid)"; \
			fi; \
			rm -f logs/extraction_api.pid; \
		fi; \
		echo "Stopping normalization API..."; \
		if [ -f logs/normalization_api.pid ]; then \
			pid=$$(cat logs/normalization_api.pid); \
			if kill -0 "$$pid" 2>/dev/null; then \
				kill "$$pid" && echo "Stopped normalization API (PID $$pid)"; \
			fi; \
			rm -f logs/normalization_api.pid; \
		fi; \
	fi
	@echo "✅ All pipeline services stopped."


