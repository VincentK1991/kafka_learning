import json
import time
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import make_asgi_app

from shared.config import get_config
from shared.metrics import MESSAGES_PROCESSED, PROCESSING_TIME

app = FastAPI()
config = get_config()

# Add prometheus asgi middleware to route /metrics requests
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


class TextData(BaseModel):
    title: str
    text: str


async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()


@app.post("/ingest")
async def ingest_text(text_data: TextData):
    start_time = time.time()
    producer = AIOKafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        message = {"title": text_data.title, "content": text_data.text}
        await producer.send_and_wait(
            config.KAFKA_TEXT_INGESTION_TOPIC, json.dumps(message).encode("utf-8")
        )
        MESSAGES_PROCESSED.labels(service='ingestion', status='success').inc()
    except Exception as e:
        MESSAGES_PROCESSED.labels(service='ingestion', status='failure').inc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await producer.stop()
        duration = time.time() - start_time
        PROCESSING_TIME.labels(service='ingestion').observe(duration)

    return {"status": "text ingested"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("text_ingestion_api:app", host="0.0.0.0", port=8001, reload=True)
