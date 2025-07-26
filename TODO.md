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
