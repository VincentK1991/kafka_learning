# Kafka AI Request Pipeline

A production-ready AI-powered async queue system built with FastAPI microservices. This pipeline demonstrates:
- **🤖 AI Request Processing**: Submit questions and get OpenAI-powered responses via async queue
- **⚡ Microservices Architecture**: FastAPI-based producer, consumer, and AI agent services  
- **📨 Event Streaming**: Using Apache Kafka for real-time AI request queuing
- **🔄 Async Processing**: AI agent continuously polls for pending requests and processes them
- **🗄️ Database Integration**: Full request lifecycle tracking in PostgreSQL
- **🐳 Containerization**: Complete Docker deployment with load balancing
- **📊 Monitoring**: Prometheus metrics, health checks, and real-time dashboards

## ✨ **AI Request Flow**

```
User -> [Submit Question] -> [Producer API] -> [Kafka Queue] -> [Consumer] -> [PostgreSQL (pending)]
                                                                                     ↓
User <- [Get Answer] <- [Producer API] <- [PostgreSQL (completed)] <- [AI Agent] <- [OpenAI API]
```

## Architecture

```
[Users] -> [Load Balancer] -> [Producer API] -> [Kafka Topic] -> [Consumer API] -> [PostgreSQL]
              |                     |               |               |                  |
          Nginx Proxy          AI Requests      Event Queue   WebSocket Streams   Request Storage
                                   |                                                     |
                              [AI Agent] <---> [OpenAI API]                        [AI Responses]
                                   |                                                     |
                               Polling Loop                                       Status Updates
```

### Microservices:
1. **Producer API** (`producer_api.py`) - HTTP API for AI requests and event ingestion
2. **Consumer API** (`consumer_api.py`) - Processes events and stores AI requests in database
3. **AI Agent** (`ai_agent.py`) - Continuously polls for pending AI requests and processes them with OpenAI
4. **Kafka** - Message queue for AI request events and data streaming  
5. **PostgreSQL** - Stores AI requests, responses, and analytics data
6. **Nginx** - Load balancer and API gateway for all services
7. **Kafka UI** - Web interface for Kafka monitoring

## Features

- 🤖 **AI Request Processing**: Submit questions, get OpenAI-powered responses via async queue
- ⚡ **Async Queue Management**: Kafka-based queuing with continuous AI agent polling
- 📊 **Request Lifecycle Tracking**: Full visibility from submission to completion
- 🚀 **Production-Ready**: FastAPI microservices with proper error handling
- 📋 **Data Validation**: Pydantic models for robust AI request and event validation  
- 🐳 **Full Containerization**: Docker deployment with complete service orchestration
- 📈 **Real-time Monitoring**: Live dashboards, AI processing metrics, and streaming data
- 🔍 **Comprehensive Observability**: Prometheus metrics, health checks, WebSocket streams
- ⚖️ **Load Balancing**: Nginx proxy for high availability across all services
- 🛡️ **API Security**: Built-in rate limiting and CORS support
- 📖 **Auto Documentation**: Interactive API docs with OpenAPI/Swagger for all services
- 🎯 **Learning Focused**: Production patterns for modern AI-powered data engineering

## Prerequisites

- **Python 3.12+** (for local development)
- **[uv](https://github.com/astral-sh/uv)** (modern Python package manager)
- **Docker and Docker Compose** (for containerized deployment)
- **OpenAI API Key** (for AI functionality)

> 📦 **No separate PostgreSQL installation needed!** The pipeline includes a containerized PostgreSQL database.

## Quick Start

### 1. Clone and Setup

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create virtual environment and install dependencies
uv venv --python 3.12
source .venv/bin/activate
uv sync

# Or use the automated setup script
chmod +x start_pipeline.sh
./start_pipeline.sh
```

### 2. Start All Services with Docker

```bash
# Start PostgreSQL, Kafka, Zookeeper, and all APIs
docker-compose up -d

# Verify all containers are running
docker-compose ps

# Database tables are created automatically!
```

> 🚀 **Everything is automated!** The PostgreSQL database, tables, and indexes are created automatically when the containers start.

### 3. Run the Pipeline

**Option A: Full Docker Deployment (Recommended)**
```bash
# Build and start all services
make build-images
make start-all

# Or using docker-compose directly
docker-compose up -d
```

**Option B: Local Development**
```bash
# Terminal 1 - Start Kafka infrastructure
make start-kafka

# Terminal 2 - Start Producer API
make start-api-producer

# Terminal 3 - Start Consumer API  
make start-api-consumer

# Terminal 4 - Monitor Pipeline
make status
```

### 5. Setup OpenAI (Required for AI Features)

```bash
# Create .env file with your OpenAI API key
echo "OPENAI_API_KEY=your_openai_api_key_here" > .env

# The AI agent service will use this key to process requests
```

### 6. Access the Services

- **Producer API**: http://localhost:8001/docs (includes AI endpoints)
- **Consumer API**: http://localhost:8002/docs  
- **AI Agent Service**: http://localhost:8003/docs
- **PostgreSQL Database**: localhost:5432 (kafka_user/kafka_password)
- **API Gateway**: http://localhost/api/producer/docs
- **Kafka UI**: http://localhost:8080
- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **WebSocket Stream**: ws://localhost:8002/ws/events

Quick access to all monitoring dashboards:
```bash
make monitoring  # Opens all dashboards in browser
```

## Configuration

Edit `config.py` to customize settings:

```python
# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_NAME = 'user_events'

# PostgreSQL Settings (containerized database)
POSTGRES_HOST = 'postgres'  # 'localhost' for local development
POSTGRES_PORT = 5432
POSTGRES_DB = 'kafka_pipeline'
POSTGRES_USER = 'kafka_user'
POSTGRES_PASSWORD = 'kafka_password'
```

## API Usage

### Producer API Endpoints

Send events via HTTP POST to the Producer API:

```bash
# Single event ingestion
curl -X POST http://localhost:8001/events \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": 1234,
    "event_type": "purchase",
    "properties": {
      "product_id": 456,
      "amount": 99.99,
      "currency": "USD",
      "category": "electronics"
    }
  }'

# Batch event ingestion
curl -X POST http://localhost:8001/events/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"user_id": 1234, "event_type": "login"},
    {"user_id": 5678, "event_type": "page_view", "properties": {"page_url": "https://example.com"}}
  ]'

# Generate sample event (for testing)
curl -X POST "http://localhost:8001/events/sample?event_type=purchase&user_id=1234"
```

### Consumer API Endpoints

Monitor and manage the pipeline:

```bash
# Pipeline status
curl http://localhost:8002/status

# Event statistics
curl http://localhost:8002/stats

# Analytics summary
curl http://localhost:8002/analytics

# Sample data
curl http://localhost:8002/sample-data?table=transformed_events&limit=5

# Health check
curl http://localhost:8002/health
```

### 🤖 AI Request API Endpoints

Submit AI requests and track their progress:

```bash
# Submit an AI request
curl -X POST http://localhost:8001/ai/ask \
  -H "Content-Type: application/json" \
  -d '{
    "question": "What is the capital of France?",
    "context": "Please provide a brief answer",
    "model": "gpt-3.5-turbo",
    "max_tokens": 100,
    "temperature": 0.7
  }' \
  -G --data-urlencode "user_id=1234"

# Check AI request status
curl http://localhost:8001/ai/requests/REQUEST_ID_HERE

# Get all AI requests for a user
curl http://localhost:8001/ai/requests/user/1234

# Check AI agent status
curl http://localhost:8003/status

# AI agent health check
curl http://localhost:8003/health

# Pause AI processing (admin)
curl -X POST http://localhost:8003/admin/pause

# Resume AI processing (admin)
curl -X POST http://localhost:8003/admin/resume
```

**Example AI Request Response:**
```json
{
  "success": true,
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "AI request submitted successfully", 
  "estimated_wait_time_seconds": 30
}
```

**Example AI Status Response:**
```json
{
  "request_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "question": "What is the capital of France?",
  "answer": "The capital of France is Paris.",
  "created_at": "2024-01-15T10:30:00Z",
  "completed_at": "2024-01-15T10:30:15Z",
  "processing_time_seconds": 12.5
}
```

### Event Types

The system supports these validated event types:

- **login/logout** - User authentication events
- **page_view** - Website navigation tracking  
- **purchase** - E-commerce transactions with revenue data
- **signup** - New user registrations with profile data
- **profile_update** - User profile modifications

## Data Schema

### Raw Events Table (`user_events`)
```sql
CREATE TABLE user_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE,
    user_id INTEGER,
    event_type VARCHAR(100),
    timestamp TIMESTAMP,
    user_agent TEXT,
    ip_address INET,
    session_id VARCHAR(255),
    properties JSONB
);
```

### Transformed Events Table (`transformed_events`)
```sql
CREATE TABLE transformed_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE,
    user_id INTEGER,
    event_type VARCHAR(100),
    event_date DATE,
    event_hour INTEGER,
    is_weekend BOOLEAN,
    revenue DECIMAL(10,2),
    country VARCHAR(100),
    category VARCHAR(100)
);
```

## Pipeline Management

Use `pipeline_manager.py` for common tasks:

```bash
# Check overall pipeline status
python pipeline_manager.py status

# View sample data
python pipeline_manager.py sample-data

# Get analytics summary
python pipeline_manager.py analytics

# Check Kafka topics
python pipeline_manager.py check-kafka

# Check database connection
python pipeline_manager.py check-db
```

## Monitoring

### Kafka UI
Access the Kafka UI at `http://localhost:8080` to:
- View topics and partitions
- Monitor message throughput
- Browse message contents

### Database Queries

```sql
-- Event distribution by type
SELECT event_type, COUNT(*) as count 
FROM transformed_events 
GROUP BY event_type;

-- Revenue by hour
SELECT event_hour, SUM(revenue) as total_revenue
FROM transformed_events 
WHERE revenue > 0
GROUP BY event_hour;

-- Weekend vs weekday activity
SELECT is_weekend, COUNT(*) as events, SUM(revenue) as revenue
FROM transformed_events 
GROUP BY is_weekend;
```

## Learning Exercises

### Beginner
1. Modify the producer to generate different event types
2. Add new fields to the transformation logic
3. Create custom analytics queries

### Intermediate
1. Implement batch processing in the consumer
2. Add error handling and retry mechanisms
3. Create multiple topics for different event types

### Advanced
1. Implement exactly-once processing semantics
2. Add schema evolution with Avro/JSON Schema
3. Build a real-time dashboard using the transformed data

## Troubleshooting

### Kafka Connection Issues
```bash
# Check if Kafka is running
docker-compose logs kafka

# Restart Kafka services
docker-compose restart
```

### Database Connection Issues
```bash
# Test database connection
python pipeline_manager.py check-db

# Check PostgreSQL logs
tail -f /usr/local/var/log/postgresql.log
```

### Common Issues

1. **Topic doesn't exist**: The producer will auto-create topics
2. **Database connection failed**: Check PostgreSQL credentials in `config.py`
3. **Import errors**: Install dependencies with `uv sync`
4. **uv not found**: Install with `curl -LsSf https://astral.sh/uv/install.sh | sh`

## Development with uv

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable dependency management with lockfile support:

```bash
# Create virtual environment with Python 3.12
uv venv --python 3.12
source .venv/bin/activate

# Install project dependencies
uv sync

# Install with development dependencies
uv sync --extra dev

# Run scripts directly
uv run python services/producer/producer_api.py
uv run python services/consumer/consumer_api.py
uv run python pipeline_manager.py status

# Or use the installed commands
kafka-producer-server
kafka-consumer-server
kafka-pipeline status

# Add new dependencies to pyproject.toml manually, then:
uv sync

# Format code with black
uv run black .

# Run type checking
uv run mypy .

# Run tests (when added)
uv run pytest
```

### Why uv sync?

- **Lockfile Support**: Creates `uv.lock` for reproducible installations
- **Faster Installs**: Up to 10-100x faster than pip
- **Conflict Resolution**: Better dependency resolution than pip
- **Project-Aware**: Automatically uses pyproject.toml configuration
- **Virtual Environment**: Manages .venv automatically

## Monitoring & Observability

### Complete Monitoring Stack

1. **Grafana Dashboards**: http://localhost:3000 (admin/admin)
   - Pre-configured dashboard for Kafka pipeline metrics
   - Real-time event rates, processing times, and health status
   - Visual charts for event distribution and performance

2. **Prometheus**: http://localhost:9090
   - Metrics collection and querying
   - Scrapes metrics from all services every 5-15 seconds
   - PromQL queries for custom analytics

3. **FastAPI Documentation**: Interactive API docs
   - Producer: http://localhost:8001/docs
   - Consumer: http://localhost:8002/docs

4. **Kafka UI**: http://localhost:8080
   - Visual Kafka cluster management
   - Topic and partition monitoring

5. **Node Exporter**: System metrics (CPU, memory, disk)
   - Metrics available at http://localhost:9100/metrics

6. **WebSocket Streaming**: Real-time event monitoring
   ```javascript
   const ws = new WebSocket('ws://localhost:8002/ws/events');
   ws.onmessage = (event) => console.log(JSON.parse(event.data));
   ```

### Available Metrics

**Producer API** (`/metrics`):
- `events_received_total{event_type, status}` - Events ingested by type and status
- `events_processing_seconds` - Event processing time histogram
- `kafka_send_seconds` - Kafka send time histogram
- `producer_health` - Producer health status (1=healthy, 0=unhealthy)

**Consumer API** (`/metrics`):
- `events_consumed_total{event_type, status}` - Events processed by type and status
- `consumer_processing_seconds` - Consumer processing time
- `database_operations_total{operation, status}` - Database operation metrics
- `consumer_health` - Consumer health status
- `consumer_lag` - Consumer lag (messages behind)

**System Metrics** (Node Exporter):
- CPU, memory, disk usage
- Network and filesystem metrics
- System load and uptime

### Quick Monitoring Setup

```bash
# Start all services including monitoring
make start-all

# Open all dashboards
make monitoring

# Check raw metrics
curl http://localhost:8001/metrics  # Producer
curl http://localhost:8002/metrics  # Consumer
curl http://localhost:9100/metrics  # System
```

## File Structure

```
kafka_learning/
├── services/              # Microservices (organized by service)
│   ├── producer/          # Producer microservice
│   │   ├── Dockerfile     # Producer container definition
│   │   └── producer_api.py # FastAPI producer service (includes AI endpoints)
│   ├── consumer/          # Consumer microservice
│   │   ├── Dockerfile     # Consumer container definition
│   │   └── consumer_api.py # FastAPI consumer service
│   ├── ai-agent/          # AI Agent microservice
│   │   ├── Dockerfile     # AI Agent container definition
│   │   └── ai_agent.py    # FastAPI AI processing service
│   ├── nginx/             # Load balancer service
│   │   └── nginx.conf     # Nginx configuration (includes AI routing)
│   └── prometheus/        # Monitoring service
│       ├── prometheus.yml # Prometheus configuration
│       └── prometheus-rules.yml # Alert rules
├── shared/               # Shared components
│   ├── models.py         # Pydantic data models
│   ├── config.py         # Configuration settings
│   └── consumer.py       # Database & transformation logic
├── scripts/              # Legacy CLI scripts
│   ├── producer.py       # CLI event producer
│   ├── consumer.py       # CLI event consumer
│   └── pipeline_manager.py # Management utilities
├── grafana/              # Grafana configuration
│   ├── datasources/      # Prometheus datasource config
│   └── dashboards/       # Pre-built dashboards
├── docker-compose.yml    # Full service orchestration
├── pyproject.toml        # Project configuration & dependencies
├── start_pipeline.sh     # Automated setup script
├── Makefile             # Development commands
├── .env                  # Environment variables (create from .env.example)
└── README.md            # This file
```

### Architecture Benefits

**🏗️ Microservices Organization**
- Each service has its own folder with Dockerfile and code
- Clear separation of concerns and responsibilities
- Easy to scale, test, and deploy services independently

**📦 Shared Components**
- Common code (models, config, database) in `shared/`
- Prevents code duplication across services
- Centralized configuration and data models

**🔧 Development Tools**
- Legacy CLI scripts preserved in `scripts/`
- Management utilities for local development
- Docker configuration organized by service

## Production Deployment

This architecture is production-ready with these considerations:

### Scaling
- **Horizontal Scaling**: Deploy multiple instances behind load balancers
- **Kafka Partitioning**: Events auto-partition by user_id for scalability
- **Database Connection Pooling**: Configure pgbouncer for PostgreSQL
- **Container Orchestration**: Use Kubernetes for auto-scaling

### Security
- **Authentication**: Add JWT/OAuth2 to FastAPI endpoints
- **Network Security**: Configure VPCs and security groups
- **Secrets Management**: Use environment variables and secret stores
- **Rate Limiting**: Configure nginx and FastAPI rate limiting

### Monitoring & Alerting
- **Prometheus + Grafana**: Full metrics and alerting stack
- **Distributed Tracing**: Add Jaeger/Zipkin for request tracing  
- **Log Aggregation**: ELK stack or similar for centralized logging
- **Health Checks**: Kubernetes liveness/readiness probes

### High Availability
- **Multi-AZ Deployment**: Deploy across availability zones
- **Database Replication**: PostgreSQL primary/replica setup
- **Kafka Clustering**: Multi-broker Kafka cluster
- **Circuit Breakers**: Add resilience patterns

## Next Steps

- **Schema Registry**: Add Confluent Schema Registry for schema evolution
- **Stream Processing**: Integrate with Kafka Streams or Apache Flink  
- **Kubernetes**: Create Helm charts for K8s deployment
- **Data Lake**: Export to S3/Delta Lake for historical analytics
- **Machine Learning**: Add real-time ML inference pipeline

## Contributing

This is a learning project! Feel free to:
- Add new event types
- Improve error handling
- Add more transformation logic
- Create additional analytics queries

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [kafka-python Library](https://kafka-python.readthedocs.io/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [Docker Compose Reference](https://docs.docker.com/compose/) 