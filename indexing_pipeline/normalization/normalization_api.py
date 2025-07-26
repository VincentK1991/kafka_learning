import asyncio
import json
import os
import time
from prometheus_client import start_http_server

from aiokafka import AIOKafkaConsumer

from indexing_pipeline.normalization.normalization_functions import (
    check_entity_exists,
    normalize_entity,
    normalize_relationship,
)
from shared.config import get_config
from shared.metrics import MESSAGES_PROCESSED, PROCESSING_TIME

config = get_config()


async def normalization_pipeline():
    consumer = AIOKafkaConsumer(
        config.KAFKA_EXTRACTION_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id="normalization_group",
    )
    
    # Allow metrics port to be configurable
    metrics_port = int(os.environ.get('METRICS_PORT', 8003))
    start_http_server(metrics_port)
    print(f"Prometheus server started on port {metrics_port}")

    print("Starting Kafka consumer...")
    await consumer.start()
    print("Normalization pipeline started successfully!")
    try:
        async for msg in consumer:
            start_time = time.time()
            try:
                message_data = json.loads(msg.value.decode("utf-8"))
                print(f"Processing message: {message_data['entity_ids']}")

                for entity_id in message_data["entity_ids"]:
                    print(f"Processing entity: {entity_id}")
                    exists = await check_entity_exists(entity_id)
                    if exists:
                        _ = await normalize_entity(entity_id)
                    exists = await check_entity_exists(entity_id)
                    if exists:
                        _ = await normalize_relationship(entity_id)

                print(f"✅ Finished processing: {message_data['entity_ids']}")
                MESSAGES_PROCESSED.labels(service='normalization', status='success').inc()

            except Exception as e:
                print(f"❌ Error processing message: {e}")
                MESSAGES_PROCESSED.labels(service='normalization', status='failure').inc()
                import traceback

                print(traceback.format_exc())
            finally:
                duration = time.time() - start_time
                PROCESSING_TIME.labels(service='normalization').observe(duration)

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(normalization_pipeline())
