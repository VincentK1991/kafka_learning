import json

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException

from shared.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TEXT_INGESTION_TOPIC

app = FastAPI()


async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.post("/ingest/")
async def ingest_text(title: str, text: str):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        message = {"title": title, "content": text}
        await producer.send_and_wait(
            KAFKA_TEXT_INGESTION_TOPIC, json.dumps(message).encode("utf-8")
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await producer.stop()
    return {"status": "text ingested"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
