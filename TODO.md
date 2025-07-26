# TODO: Implement Saga Pattern for Transactional Integrity

This document outlines the steps to implement the Saga pattern for the multi-step Kafka pipeline (Ingestion -> Extraction -> Normalization). The goal is to ensure that if a step fails, all previous operations in the same transaction are rolled back, maintaining data consistency in the Neo4j database.

We will use a **Choreography-based Saga**, where services communicate through events (Kafka messages) to trigger subsequent steps or rollbacks.

---

### Phase 1: Core Saga Implementation

#### 1. Introduce a Global Transaction ID (`saga_id`)

The first step is to create a unique ID at the start of the process that can be used to track the entire transaction lifecycle.

-   **File**: `indexing_pipeline/inputs/text_ingestion_api.py`
-   **Action**:
    -   In the `ingest_text` function, generate a unique ID (e.g., using `uuid.uuid4()`).
    -   This `saga_id` must be included in the first message sent to the `text_ingestion` topic.

#### 2. Update Kafka Message Schemas

All messages related to the saga must carry the `saga_id` and have a consistent structure.

-   **Action**: Define a standard message format.
    ```json
    {
      "saga_id": "unique-transaction-id-123",
      "status": "SUCCESS" | "FAILURE",
      "source_service": "extraction_api",
      "payload": { ... },
      "error_details": "..." // Optional: only if status is FAILURE
    }
    ```
-   This structure should be applied to messages in all topics (`text_ingestion`, `extraction`, and the new rollback topics).

#### 3. Create New Kafka Topics for Rollbacks

We need dedicated topics for coordinating the rollback process.

-   **File**: `shared/config.py`
-   **Action**: Add new topic configurations:
    -   `KAFKA_EXTRACTION_ROLLBACK_TOPIC`: To trigger a rollback of the extraction step.
    -   `KAFKA_NORMALIZATION_ROLLBACK_TOPIC`: To trigger a rollback of the normalization step.

#### 4. Modify `extraction_api` for Saga Logic

This service needs to handle both the forward (success) path and the compensation (rollback) path.

-   **File**: `indexing_pipeline/extraction/extraction_api.py`
-   **Actions**:
    1.  **Modify the main consumer (`extraction_pipeline`)**:
        -   In the `try` block, after successfully processing a message and writing to Neo4j:
            -   Produce a **SUCCESS** message to the `KAFKA_EXTRACTION_TOPIC` containing the `saga_id` and the `entity_ids` in the payload.
        -   In the `except` block (if `extract_embed_index_data` fails):
            -   Produce a **FAILURE** message to the `KAFKA_EXTRACTION_ROLLBACK_TOPIC`. This message doesn't need to trigger any action but is useful for monitoring.
    2.  **Create a new consumer for rollbacks**:
        -   This new consumer function must listen to the `KAFKA_NORMALIZATION_ROLLBACK_TOPIC`.
        -   When it receives a message, it means the *normalization* step failed.
        -   It must execute a **compensating transaction**: Use the `entity_ids` from the rollback message's payload to run `MATCH (n) WHERE n.id IN $ids DETACH DELETE n` Cypher queries to delete the data it created in Neo4j.

#### 5. Modify `normalization_api` for Saga Logic

This service is the last step in the happy path, so its main job is to trigger rollbacks if it fails.

-   **File**: `indexing_pipeline/normalization/normalization_api.py`
-   **Actions**:
    -   **Modify the main consumer (`normalization_pipeline`)**:
        -   In the `try` block for each entity:
            -   If successful, the saga for that branch is complete. You could optionally produce a `saga_success` event for monitoring.
        -   In the `except` block (if `normalize_entity` or `normalize_relationship` fails):
            -   Produce a **FAILURE** message to the `KAFKA_NORMALIZATION_ROLLBACK_TOPIC`.
            -   The message payload **must** contain the `saga_id` and the `entity_ids` that the `extraction_api` needs to roll back its changes.

---

### Phase 2: Advanced Improvements & Considerations

#### 1. Implement a More Robust Normalization Rollback

-   **Challenge**: The current normalization logic modifies data in place (`MERGE` queries). A simple `DELETE` is not a correct rollback.
-   **Action (Future)**:
    -   Before running a normalization query, fetch the state of the nodes/relationships that will be affected.
    -   The compensating action would be to restore this previous state. This is complex and requires careful planning.
    -   A simpler, state-less approach could be to add a "version" or "last_normalized_by" property to nodes/relationships.

#### 2. Dead Letter Queues (DLQs)

-   **Challenge**: If a message consistently fails (e.g., due to malformed data), it can cause an infinite loop of processing and rollbacks.
-   **Action**:
    -   In each service's error handling, implement a retry counter.
    -   If a message fails more than N times, send it to a dedicated Dead Letter Queue (e.g., `extraction_dlq`) for manual inspection.

#### 3. Saga State Monitoring

-   **Challenge**: With a choreography-based saga, it's hard to see the status of a transaction at a glance.
-   **Action**:
    -   Create a new service that consumes from all relevant topics (`extraction`, `extraction_rollback`, etc.).
    -   This service would build and maintain a state machine in a database (e.g., PostgreSQL or a dedicated collection in Neo4j) to track the status of each `saga_id`: `IN_PROGRESS`, `COMPLETED`, `FAILED`, `ROLLED_BACK`.

---
## Observability and Monitoring Plan

This section details how to leverage the existing Prometheus and Grafana setup to build comprehensive monitoring for the pipeline.

### Step 1: Instrument Python Services with Metrics

The most critical step is to make our Python services expose metrics that Prometheus can scrape.

-   **Library**: Add `prometheus-client` to `pyproject.toml` and run `uv sync`.
-   **Action**: In each of the three services (`text_ingestion_api`, `extraction_api`, `normalization_api`), expose a `/metrics` endpoint. For FastAPI, this is straightforward. For the consumer services, you can run the metrics server in a separate thread.

-   **Metrics to Expose**:
    -   **Counter**: `pipeline_messages_total{service, status}`
        -   `service`: "extraction", "normalization", etc.
        -   `status`: "success" or "failure".
        -   *Increment this for every message processed.*
    -   **Histogram**: `pipeline_processing_duration_seconds{service}`
        -   *Record the time it takes to process each message.*
    -   **Counter**: `pipeline_rollbacks_total{service}`
        -   *Increment when a rollback is initiated or processed.*
    -   **Gauge**: `kafka_consumer_lag{group_id, topic, partition}`
        -   *Expose the consumer lag. `aiokafka` provides ways to get this information.*

### Step 2: Add a Kafka Exporter

The JMX port (`9101`) provides deep metrics, but it's easier to use a dedicated Kafka exporter that translates these into a Prometheus-friendly format.

-   **Action**: Add a Kafka exporter service to `docker-compose.neo4j_infrastructure.yml`.
    ```yaml
    services:
      # ... other services
      kafka-exporter:
        image: danielqsj/kafka-exporter:v1.7.0
        container_name: kafka-exporter
        command: --kafka.server=kafka:29092
        ports:
          - "9308:9308"
        networks:
          - kafka-network
        restart: unless-stopped
    ```
-   **Action**: Update `services/prometheus/prometheus.yml` to scrape this new service.
    ```yaml
    # In scrape_configs:
    - job_name: 'kafka-exporter'
      static_configs:
        - targets: ['kafka-exporter:9308']
    ```

### Step 3: Add a Neo4j Exporter

To get insights into the database, we need to expose its metrics.

-   **Action**: Add the official Neo4j Prometheus plugin. In `docker-compose.neo4j_infrastructure.yml`, modify the `neo4j` service environment variables:
    ```yaml
    # In the neo4j service:
    environment:
      NEO4J_AUTH: "neo4j/password"
      NEO4J_PLUGINS: '["apoc", "graph-data-science", "prometheus"]' # Add prometheus
      NEO4J_ACCEPT_LICENSE_AGREEMENT: "yes"
      # Add this line to enable the exporter endpoint
      NEO4J_metrics_prometheus_enabled: "true"
    ```
-   **Action**: Update `services/prometheus/prometheus.yml` to scrape Neo4j.
    ```yaml
    # In scrape_configs:
    - job_name: 'neo4j'
      static_configs:
        - targets: ['neo4j:2004'] # Default port for the Neo4j exporter
    ```

### Step 4: Build a Custom Grafana Dashboard

With all the new metrics available, create a dedicated dashboard to visualize the pipeline's health.

-   **File**: Create a new JSON file in `grafana/dashboards/pipeline.json`.
-   **Action**: Add panels to the dashboard:
    1.  **Pipeline Health**:
        -   **Messages Processed**: A graph showing the `rate()` of `pipeline_messages_total` for each service, faceted by `status`. This gives you a quick look at throughput and error rates.
        -   **Processing Latency**: A heatmap or histogram of `pipeline_processing_duration_seconds` (e.g., p95, p99) for each service.
    2.  **Kafka Monitoring**:
        -   **Consumer Lag**: A graph of `kafka_consumergroup_lag` from the Kafka exporter, showing how far behind each consumer group is. This is the most important metric for queue health.
        -   **Topic Throughput**: A graph of `kafka_topic_partitions_messages` to see the rate of messages being written to each topic.
    3.  **Saga & Rollbacks**:
        -   **Rollback Rate**: A graph showing the `rate()` of `pipeline_rollbacks_total`. Any number greater than zero here deserves attention.
    4.  **Neo4j Health**:
        -   **Transaction Rate**: A graph of `neo4j_transaction_total` from the Neo4j exporter.
        -   **Database Size**: Gauges for `neo4j_store_size_total_bytes` and counts for nodes and relationships.