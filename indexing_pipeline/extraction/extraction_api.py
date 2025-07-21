import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from index_function import create_indices

from indexing_pipeline.extraction.extraction_functions import extract_embed_index_data
from shared.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_EXTRACTION_TOPIC,
    KAFKA_TEXT_INGESTION_TOPIC,
)


async def extraction_pipeline():
    consumer = AIOKafkaConsumer(
        KAFKA_TEXT_INGESTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="extraction_group",
    )
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    await create_indices()
    await consumer.start()
    await producer.start()
    try:
        async for msg in consumer:
            message_data = json.loads(msg.value.decode("utf-8"))

            extraction_result = await extract_embed_index_data(
                message_data["title"], message_data["content"]
            )

            await producer.send_and_wait(
                KAFKA_EXTRACTION_TOPIC, json.dumps(extraction_result).encode("utf-8")
            )
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(extraction_pipeline())
