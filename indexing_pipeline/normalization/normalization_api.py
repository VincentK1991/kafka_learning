import asyncio
import json

from aiokafka import AIOKafkaConsumer

from shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_EXTRACTION_TOPIC


async def normalization_pipeline():
    consumer = AIOKafkaConsumer(
        KAFKA_EXTRACTION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="normalization_group",
    )
    await consumer.start()
    try:
        async for msg in consumer:
            message_data = json.loads(msg.value.decode("utf-8"))

            # Blank normalization logic
            print(f"Normalized data: {message_data}")

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(normalization_pipeline())
