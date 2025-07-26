# Kafka-Powered Knowledge Graph Ingestion Pipeline

This project demonstrates a production-ready, asynchronous pipeline for ingesting unstructured text, extracting structured information as a knowledge graph, and storing it in a Neo4j database. The entire system is built with Python, FastAPI, and Kafka, and is fully containerized with Docker.

- 🧠 Knowledge Graph Construction: Ingest raw text and automatically build a rich, interconnected knowledge graph.
- ⚡ Microservices Architecture: A multi-stage pipeline with independent services for ingestion, extraction, and normalization.
- 📨 Event-Driven with Kafka: Services communicate asynchronously using Kafka topics, ensuring scalability and resilience.
- 🤖 AI-Powered Extraction: Leverages advanced AI models (via `gpt-4.1-mini`) for high-quality entity and relationship extraction.
- 🕸️ Graph Database: Uses Neo4j to store and query the complex, interconnected data of the knowledge graph.
- 🔄 Data Normalization: Includes a final stage to de-duplicate and clean the graph data, ensuring high quality.
- 🐳 Fully Containerized: All components, including Kafka and Neo4j, are managed with Docker Compose for easy setup.
- 📊 Monitoring Ready: Includes services like Prometheus and Grafana for observing the pipeline's health and performance.

## ✨ Pipeline Flow

The pipeline processes data in three main stages, with each stage communicating via Kafka:

```
[User/Client]
      |
      | POST /ingest
      v
[1. Ingestion API] --(produces to)--> [Kafka Topic: text_ingestion]
                                                 |
                                          (consumed by)
                                                 |
                                                 v
[2. Extraction Service] --(produces to)--> [Kafka Topic: extraction]
        |         ^
        | (writes graph data)
        v         |
      [Neo4j] <---(normalizes data)---- [3. Normalization Service]

```

1.  Ingestion: A FastAPI endpoint receives raw text and sends it to the `text_ingestion` Kafka topic.
2.  Extraction: A consumer service listens to `text_ingestion`. It chunks and embeds the text, uses an AI agent to extract entities and relationships, and writes this initial graph data to Neo4j. It then sends the IDs of the new entities to the `extraction` topic.
3.  Normalization: A final consumer service listens to `extraction`. It runs queries against Neo4j to de-duplicate the entities and relationships created in the previous step, keeping the knowledge graph clean and consistent.

## Prerequisites

- Python 3.12+ (for local development)
- [uv](https://github.com/astral-sh/uv) (a fast Python package manager)
- Docker and Docker Compose
- OpenAI API Key (for the AI extraction service)

## Quick Start

### 1. Environment Setup

```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create a virtual environment and install dependencies
uv venv
source .venv/bin/activate
uv sync

# Create a .env file with your OpenAI API key
echo "OPENAI_API_KEY=your_openai_api_key_here" > .env
```

### 2. Start Infrastructure with Docker

This command starts Kafka, Zookeeper, Neo4j, and monitoring services.

```bash
# Use the neo4j-infrastructure compose file
docker-compose -f docker-compose.neo4j_infrastructure.yml up -d

# Verify all containers are running
docker-compose -f docker-compose.neo4j_infrastructure.yml ps
```

### 3. Run the Pipeline Services

You need to run each of the three pipeline services in a separate terminal.

Terminal 1 - Start Ingestion API:
```bash
uv run python indexing_pipeline/inputs/text_ingestion_api.py
```

Terminal 2 - Start Extraction Service:
```bash
uv run python indexing_pipeline/extraction/extraction_api.py
```

Terminal 3 - Start Normalization Service:
```bash
uv run python indexing_pipeline/normalization/normalization_api.py
```

### 4. Use the Pipeline

- Ingestion API: `http://localhost:8001/docs`
- Neo4j Browser: `http://localhost:7474` (Use user `neo4j` and password `password`)
- Kafka UI: `http://localhost:8080`
- Grafana: `http://localhost:3000`

Send data for ingestion:
```bash
curl -X POST http://localhost:8001/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Apple Q1 2024 Results",
    "text": "Apple announced record revenue of $119.6 billion for the first quarter of 2024. The iPhone product line, led by the iPhone 15, was a significant contributor. Tim Cook, the CEO, mentioned strong performance in the services division as well."
  }'
```

You can then use the Neo4j Browser to see the created graph and the Kafka UI to monitor the messages flowing through the topics.

## Kafka Explained: Topics, Producers, and Consumers

This project relies heavily on Apache Kafka. Here are the core concepts:

-   Topic: A topic is a named stream of messages in Kafka. Think of it like a category or a channel. Producers write messages to topics, and consumers read messages from them. In this project, you have topics like `text_ingestion` and `extraction` that act as buffers between the different processing stages.

-   Producer: A producer is a client application that sends or publishes messages to a Kafka topic. In your pipeline, the `text_ingestion_api` is a producer because it sends the initial text to the `text_ingestion` topic. The `extraction_api` is also a producer because it sends the results of its work to the `extraction` topic.

-   Consumer: A consumer is a client application that subscribes to one or more topics and processes the messages from them. The `extraction_api` and `normalization_api` are both consumers, as they listen for new messages on their respective topics and perform actions based on them.

### Scaling with Consumer Groups

How can we process messages faster? By running multiple instances of a consumer. This is managed through Consumer Groups.

-   Consumer Group (`group_id`): When multiple consumer instances are configured with the same `group_id`, they form a Consumer Group. Kafka intelligently distributes the partitions of a topic among the members of that group.

-   Parallel Processing: A topic can be split into multiple partitions. Each partition is assigned to exactly one consumer within the group. This allows your services to process messages in parallel. For example, if the `text_ingestion` topic has 4 partitions, you can run 4 instances of the `extraction_api` service. Kafka will assign one partition to each, allowing you to process 4 messages simultaneously and dramatically increasing throughput.

To scale up, you would:
1.  Increase topic partitions:
    ```bash
    docker-compose -f docker-compose.neo4j_infrastructure.yml exec kafka kafka-topics --bootstrap-server kafka:29092 --alter --topic text_ingestion --partitions 4
    ```
2.  Run more consumer instances: Simply open more terminals and run the consumer script. Kafka handles the rest.

```
