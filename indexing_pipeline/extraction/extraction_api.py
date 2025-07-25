import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from indexing_pipeline.extraction.embedding.index_function import create_indices
from indexing_pipeline.extraction.extraction_functions import extract_embed_index_data
from shared.config import get_config


async def extraction_pipeline():
    config = get_config()
    consumer = AIOKafkaConsumer(
        config.KAFKA_TEXT_INGESTION_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id="extraction_group",
    )
    producer = AIOKafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    print("Creating indices...")
    await create_indices()
    print("Starting Kafka consumer and producer...")
    await consumer.start()
    await producer.start()
    print("Extraction pipeline started successfully!")
    try:
        async for msg in consumer:
            try:
                message_data = json.loads(msg.value.decode("utf-8"))
                print(f"Processing message: {message_data['title']}")

                extraction_result = await extract_embed_index_data(
                    message_data["title"], message_data["content"]
                )

                await producer.send_and_wait(
                    config.KAFKA_EXTRACTION_TOPIC,
                    json.dumps(extraction_result).encode("utf-8"),
                )

                print(f"✅ Finished processing: {message_data['title']}")

            except Exception as e:
                print(f"❌ Error processing message: {e}")
                # Continue processing other messages

    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(extraction_pipeline())
