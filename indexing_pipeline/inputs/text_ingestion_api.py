import json

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.config import get_config

app = FastAPI()
config = get_config()


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
    producer = AIOKafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    await producer.start()
    try:
        message = {"title": text_data.title, "content": text_data.text}
        await producer.send_and_wait(
            config.KAFKA_TEXT_INGESTION_TOPIC, json.dumps(message).encode("utf-8")
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await producer.stop()
    return {"status": "text ingested"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("text_ingestion_api:app", host="0.0.0.0", port=8001, reload=True)
