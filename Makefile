.PHONY: help install install-dev setup start-kafka start-producer start-consumer start-api-producer start-api-consumer stop-kafka status clean format lint type-check test build-images start-all stop-all monitoring

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
	docker-compose up -d

stop-kafka: ## Stop Kafka infrastructure
	docker-compose down

db-shell: ## Connect to PostgreSQL database shell
	docker exec -it kafka-postgres psql -U kafka_user -d kafka_pipeline

db-logs: ## View PostgreSQL database logs
	docker logs kafka-postgres

db-status: ## Check database status and tables
	docker exec -it kafka-postgres psql -U kafka_user -d kafka_pipeline -c "\dt"

start-producer: ## Start the legacy CLI producer
	uv run python scripts/producer.py

start-consumer: ## Start the legacy CLI consumer
	uv run python scripts/consumer.py

start-api-producer: ## Start the FastAPI producer server
	uv run python services/producer/producer_api.py

start-api-consumer: ## Start the FastAPI consumer server
	uv run python services/consumer/consumer_api.py

start-ai-agent: ## Start the AI agent service
	uv run python services/ai-agent/ai_agent.py

start-ai-services: ## Start all AI services (producer, consumer, ai-agent)
	@echo "Starting AI-powered pipeline services..."
	@echo "Make sure to set OPENAI_API_KEY in .env file"
	@echo "Terminal 1: make start-api-producer"
	@echo "Terminal 2: make start-api-consumer" 
	@echo "Terminal 3: make start-ai-agent"

build-images: ## Build Docker images for FastAPI services
	docker build -f services/producer/Dockerfile -t kafka-producer-api .
	docker build -f services/consumer/Dockerfile -t kafka-consumer-api .
	docker build -f services/ai-agent/Dockerfile -t kafka-ai-agent .

start-all: ## Start all services with Docker Compose
	docker-compose up -d

stop-all: ## Stop all services
	docker-compose down

status: ## Check pipeline status
	uv run python pipeline_manager.py status

sample-data: ## Show sample data from database
	uv run python pipeline_manager.py sample-data

analytics: ## Show analytics summary
	uv run python pipeline_manager.py analytics

monitoring: ## Open monitoring dashboards in browser
	@echo "Opening monitoring dashboards..."
	@echo "Grafana: http://localhost:3000 (admin/admin)"
	@echo "Prometheus: http://localhost:9090"
	@echo "Kafka UI: http://localhost:8080"
	@if command -v open >/dev/null 2>&1; then \
		open http://localhost:3000 && \
		open http://localhost:9090 && \
		open http://localhost:8080; \
	elif command -v xdg-open >/dev/null 2>&1; then \
		xdg-open http://localhost:3000 && \
		xdg-open http://localhost:9090 && \
		xdg-open http://localhost:8080; \
	fi

clean: ## Clean up generated files
	rm -rf .venv/
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

format: ## Format code with black
	uv run black .

lint: ## Run flake8 linting
	uv run flake8 .

type-check: ## Run mypy type checking
	uv run mypy .

test: ## Run tests with pytest
	uv run pytest 