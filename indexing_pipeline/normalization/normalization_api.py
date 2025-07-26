import asyncio
import json

from aiokafka import AIOKafkaConsumer

from indexing_pipeline.normalization.normalization_functions import (
    check_entity_exists,
    normalize_entity,
    normalize_relationship,
)
from shared.config import get_config

config = get_config()


async def normalization_pipeline():
    consumer = AIOKafkaConsumer(
        config.KAFKA_EXTRACTION_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        group_id="normalization_group",
    )
    print("Starting Kafka consumer...")
    await consumer.start()
    print("Normalization pipeline started successfully!")
    try:
        async for msg in consumer:
            try:
                message_data = json.loads(msg.value.decode("utf-8"))
                print(f"📨 Processing message: {message_data['entity_ids']}")

                for entity_id in message_data["entity_ids"]:
                    print(f"\n🎯 Processing entity: {entity_id}")

                    try:
                        # Check if entity exists before processing
                        exists = await check_entity_exists(entity_id)
                        if not exists:
                            print(
                                f"⚠️  Entity {entity_id} does not exist - skipping normalization"
                            )
                            continue

                        # Step 1: Entity normalization
                        print("🔄 Step 1: Entity normalization...")
                        _ = await normalize_entity(entity_id)
                        print("📊 Entity normalization result")

                        # Check if entity still exists after potential merging
                        exists_after_entity_norm = await check_entity_exists(entity_id)
                        if not exists_after_entity_norm:
                            print(
                                f"⚠️  Entity {entity_id} was merged/deleted during\
                                entity normalization - skipping relationship\
                                normalization"
                            )
                            print(
                                f"✅ Finished processing entity: {entity_id} (merged)"
                            )
                            continue

                        # Step 2: Relationship normalization
                        print("🔄 Step 2: Relationship normalization...")
                        _ = await normalize_relationship(entity_id)
                        print("📊 Relationship normalization result")

                        print(f"✅ Finished processing entity: {entity_id}")

                    except Exception as e:
                        print(f"❌ Error processing entity {entity_id}: {e}")
                        print(f"🔍 Error type: {type(e).__name__}")

                        # Log the traceback for debugging but continue with other entities
                        import traceback

                        print(f"📋 Traceback: {traceback.format_exc()}")
                        print("⏭️  Continuing with next entity...")

                print(f"✅ Finished processing message: {message_data['entity_ids']}")

            except Exception as e:
                print(f"❌ Error processing message: {e}")
                import traceback

                print(traceback.format_exc())

    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(normalization_pipeline())
