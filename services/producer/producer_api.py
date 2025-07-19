#!/usr/bin/env python3
"""
FastAPI Producer Server - HTTP API for event ingestion
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import KafkaError

from shared.config import Config
from shared.database import (
    AsyncDatabaseConnector,
    close_connection_pool,
    get_db_dependency,
)
from shared.models import (
    AIRequestProperties,
    AIRequestResponse,
    AIRequestsListResponse,
    AIStatusResponse,
    BatchEventResponse,
    EventResponse,
    HealthResponse,
    event_to_dict,
    validate_event,
)

load_dotenv()

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global variables
app = FastAPI(
    title="Kafka Event Producer API",
    description="HTTP API for ingesting events into Kafka",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

producer: KafkaProducer = None
startup_time = time.time()


# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AsyncProducerManager:
    """Manages Kafka producer connections (database managed by pool)"""

    def __init__(self):
        self.producer = None
        self.connected = False

    def connect(self):
        """Connect to Kafka (database connections managed by pool)"""
        try:
            # Connect to Kafka
            self.producer = KafkaProducer(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode("utf-8"),
                key_serializer=lambda x: x.encode("utf-8") if x else None,
                acks="all",  # Wait for all replicas to acknowledge
                retries=3,  # Retry on failure
                max_in_flight_requests_per_connection=1,  # Ensure ordering
                compression_type="gzip",  # Compress messages
            )
            self.connected = True
            logger.info("Connected to Kafka successfully")

        except Exception as e:
            self.connected = False
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def disconnect(self):
        """Disconnect from Kafka"""
        if self.producer:
            self.producer.close()
            self.connected = False
            logger.info("Disconnected from Kafka")

    def send_event(self, event: dict[str, Any]) -> bool:
        """Send event to Kafka"""
        if not self.connected:
            raise HTTPException(
                status_code=503, detail="Producer not connected to Kafka"
            )

        try:
            key = str(event.get("user_id", ""))

            future = self.producer.send(Config.KAFKA_TOPIC_NAME, key=key, value=event)

            # Wait for the message to be sent (synchronous for demo purposes)
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Sent event {event['event_id']} to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Kafka error sending event {event.get('event_id')}: {e}")
            raise HTTPException(
                status_code=503, detail=f"Failed to send event: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error sending event {event.get('event_id')}: {e}")
            raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


# Global producer manager
producer_manager = AsyncProducerManager()


@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    logger.info("Starting Producer API server...")
    producer_manager.connect()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Producer API server...")
    producer_manager.disconnect()
    await close_connection_pool()


def get_producer_manager() -> AsyncProducerManager:
    """Dependency to get producer manager"""
    return producer_manager


# API Endpoints


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check(db: AsyncDatabaseConnector = Depends(get_db_dependency)):
    """Health check endpoint with async database test"""
    uptime = time.time() - startup_time

    try:
        # Test database connectivity
        result = await db.execute_query("SELECT 1 as test", fetch_mode="one")
        db_healthy = result is not None
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_healthy = False

    checks = {
        "kafka_connected": producer_manager.connected,
        "database_connected": db_healthy,
        "producer_initialized": producer_manager.producer is not None,
        "connection_pool": True,
    }

    all_healthy = all(checks.values())

    return HealthResponse(
        status="ok" if all_healthy else "degraded", uptime_seconds=uptime, checks=checks
    )


@app.get("/metrics", tags=["Monitoring"])
async def metrics():
    """Prometheus metrics endpoint"""
    # Metrics temporarily disabled
    return Response(content="# Metrics temporarily disabled\n", media_type="text/plain")


@app.post("/events", response_model=EventResponse, tags=["Events"])
async def ingest_event(
    event_data: dict[str, Any],
    producer_mgr: AsyncProducerManager = Depends(get_producer_manager),
):
    """Ingest a single event"""
    try:
        # Add event_id if not provided
        if "event_id" not in event_data:
            event_data["event_id"] = str(uuid.uuid4())

        # Add timestamp if not provided
        if "timestamp" not in event_data:
            event_data["timestamp"] = datetime.now().isoformat()

        # Validate event using Pydantic models
        validated_event = validate_event(event_data)
        event_dict = event_to_dict(validated_event)

        # Send to Kafka
        success = producer_mgr.send_event(event_dict)

        if success:
            return EventResponse(
                success=True,
                event_id=validated_event.event_id,
                message="Event ingested successfully",
            )

    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=f"Validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.post("/events/batch", response_model=BatchEventResponse, tags=["Events"])
async def ingest_batch_events(
    events: list[dict[str, Any]],
    producer_mgr: AsyncProducerManager = Depends(get_producer_manager),
):
    """Ingest multiple events in batch"""

    if len(events) > 1000:  # Limit batch size
        raise HTTPException(
            status_code=400, detail="Batch size cannot exceed 1000 events"
        )

    processed_count = 0
    failed_count = 0
    failed_events = []

    for i, event_data in enumerate(events):
        try:
            # Add event_id if not provided
            if "event_id" not in event_data:
                event_data["event_id"] = str(uuid.uuid4())

            # Add timestamp if not provided
            if "timestamp" not in event_data:
                event_data["timestamp"] = datetime.now().isoformat()

            # Validate event
            validated_event = validate_event(event_data)
            event_dict = event_to_dict(validated_event)

            # Send to Kafka
            producer_mgr.send_event(event_dict)
            processed_count += 1

        except Exception as e:
            failed_count += 1
            failed_events.append(f"Event {i}: {str(e)}")
            logger.error(f"Failed to process event {i}: {e}")

    return BatchEventResponse(
        success=failed_count == 0,
        processed_count=processed_count,
        failed_count=failed_count,
        failed_events=failed_events,
        message=f"Processed {processed_count} events, {failed_count} failed",
    )


@app.post(
    "/events/sample", response_model=EventResponse, tags=["Events", "Development"]
)
async def generate_sample_event(
    event_type: str = "page_view",
    user_id: int = None,
    producer_mgr: AsyncProducerManager = Depends(get_producer_manager),
):
    """Generate and ingest a sample event (for testing/development)"""

    import random

    from faker import Faker

    fake = Faker()

    # Generate sample event based on type
    if user_id is None:
        user_id = random.randint(1000, 9999)

    event_data = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_type": event_type,
        "timestamp": datetime.now().isoformat(),
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4(),
        "session_id": str(uuid.uuid4()),
        "properties": {},
    }

    # Add event-specific properties
    if event_type == "purchase":
        event_data["properties"] = {
            "product_id": random.randint(1, 1000),
            "amount": round(random.uniform(10.0, 500.0), 2),
            "currency": "USD",
            "category": random.choice(["electronics", "clothing", "books", "home"]),
        }
    elif event_type == "page_view":
        event_data["properties"] = {
            "page_url": fake.url(),
            "referrer": fake.url(),
            "page_title": fake.sentence(),
        }
    elif event_type == "signup":
        event_data["properties"] = {
            "email": fake.email(),
            "name": fake.name(),
            "age": random.randint(18, 80),
            "country": fake.country(),
        }

    # Use the regular event ingestion endpoint
    return await ingest_event(event_data, producer_mgr)


# AI Request Endpoints


@app.post("/ai/ask", response_model=AIRequestResponse, tags=["AI"])
async def submit_ai_request(
    ai_request: AIRequestProperties,
    user_id: int,
    producer_mgr: AsyncProducerManager = Depends(get_producer_manager),
    db: AsyncDatabaseConnector = Depends(get_db_dependency),
):
    """Submit an AI request for processing with async database operations"""
    try:
        # Create AI request event
        event_data = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": "ai_request",
            "timestamp": datetime.now().isoformat(),
            "properties": ai_request.dict(),
        }

        # Validate the event
        validated_event = validate_event(event_data)
        event_dict = event_to_dict(validated_event)

        # Store in database first to get a request_id
        request_id = await db.insert_ai_request(event_dict)

        # Prepare message for AI topic
        ai_task_payload = {
            "request_id": request_id,
            "user_id": user_id,
            "question": ai_request.question,
            "context": ai_request.context,
            "model": ai_request.model,
            "max_tokens": ai_request.max_tokens,
            "temperature": ai_request.temperature,
        }

        # Send to dedicated AI Kafka topic
        try:
            future = producer_mgr.producer.send(
                Config.KAFKA_AI_REQUESTS_TOPIC,
                key=str(user_id),
                value=ai_task_payload,
            )
            future.get(timeout=10)  # Wait for send confirmation
            logger.info(
                f"Sent AI request {request_id}\
                     to topic {Config.KAFKA_AI_REQUESTS_TOPIC}"
            )
        except KafkaError as e:
            logger.error(f"Kafka error sending AI request {request_id}: {e}")
            # Potentially roll back the DB insert or mark as failed immediately
            await db.update_ai_request_status(
                request_id, "failed", error_message=f"Kafka queuing failed: {e}"
            )
            raise HTTPException(
                status_code=503, detail=f"Failed to queue AI request: {str(e)}"
            )

        # Send original event to main topic for logging/analytics
        producer_mgr.send_event(event_dict)

        # Estimate wait time based on pending requests
        pending_requests = await db.get_pending_ai_requests(limit=1000)
        estimated_wait = (
            len(pending_requests) * 10
        )  # Rough estimate: 10 seconds per request

        return AIRequestResponse(
            success=True,
            request_id=request_id,
            message="AI request submitted successfully",
            estimated_wait_time_seconds=estimated_wait,
        )

    except Exception as e:
        logger.error(f"Error submitting AI request: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to submit AI request: {str(e)}"
        )


@app.get("/ai/requests/{request_id}", response_model=AIStatusResponse, tags=["AI"])
async def get_ai_request_status(
    request_id: str, db: AsyncDatabaseConnector = Depends(get_db_dependency)
):
    """Get the status of an AI request using async database operations"""
    try:
        request_data = await db.get_ai_request_by_id(request_id)

        if not request_data:
            raise HTTPException(status_code=404, detail="AI request not found")

        return AIStatusResponse(
            request_id=request_data["request_id"],
            status=request_data["status"],
            question=request_data["question"],
            answer=request_data.get("answer"),
            error_message=request_data.get("error_message"),
            created_at=request_data["created_at"],
            completed_at=request_data.get("completed_at"),
            processing_time_seconds=float(request_data["processing_time_seconds"])
            if request_data.get("processing_time_seconds")
            else None,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting AI request status: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get AI request status: {str(e)}"
        )


@app.get(
    "/ai/requests/user/{user_id}", response_model=AIRequestsListResponse, tags=["AI"]
)
async def get_user_ai_requests(
    user_id: int,
    limit: int = 50,
    db: AsyncDatabaseConnector = Depends(get_db_dependency),
):
    """Get AI requests for a specific user using async database operations"""
    if limit > 100:
        limit = 100  # Prevent excessive queries

    try:
        requests_data = await db.get_ai_requests_by_user(user_id, limit)

        requests = []
        pending_count = 0
        completed_count = 0

        for req in requests_data:
            ai_status = AIStatusResponse(
                request_id=req["request_id"],
                status=req["status"],
                question=req["question"],
                answer=req.get("answer"),
                error_message=req.get("error_message"),
                created_at=req["created_at"],
                completed_at=req.get("completed_at"),
                processing_time_seconds=float(req["processing_time_seconds"])
                if req.get("processing_time_seconds")
                else None,
            )
            requests.append(ai_status)

            if req["status"] == "pending":
                pending_count += 1
            elif req["status"] in ["completed", "failed"]:
                completed_count += 1

        return AIRequestsListResponse(
            requests=requests,
            total_count=len(requests),
            pending_count=pending_count,
            completed_count=completed_count,
        )

    except Exception as e:
        logger.error(f"Error getting user AI requests: {e}")
        raise HTTPException(
            status_code=500, detail=f"Failed to get user AI requests: {str(e)}"
        )


@app.get("/status", tags=["Monitoring"])
async def get_status():
    """Get producer status"""
    return {
        "status": "healthy" if producer_manager.connected else "unhealthy",
        "kafka_connected": producer_manager.connected,
        "uptime_seconds": time.time() - startup_time,
        "topic": Config.KAFKA_TOPIC_NAME,
        "bootstrap_servers": Config.KAFKA_BOOTSTRAP_SERVERS,
    }


def start_server():
    """Start the FastAPI server"""
    uvicorn.run(
        "services.producer.producer_api:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
