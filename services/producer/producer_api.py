#!/usr/bin/env python3
"""
FastAPI Producer Server - HTTP API for event ingestion
"""

import json
import logging
import time
import uuid
from datetime import datetime
from typing import List, Dict, Any

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaProducer
from kafka.errors import KafkaError
import uvicorn

from fastapi import Response

from shared.config import Config
from shared.consumer import DatabaseManager
from shared.models import (
    EventResponse,
    BatchEventResponse,
    HealthResponse,
    AIRequestProperties,
    AIRequestResponse,
    AIStatusResponse,
    AIRequestsListResponse,
    validate_event,
    event_to_dict,
)

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


class ProducerManager:
    """Manages Kafka producer and database connections"""

    def __init__(self):
        self.producer = None
        self.db_manager = None
        self.connected = False
        self.db_connected = False

    def connect(self):
        """Connect to Kafka and Database"""
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

            # Connect to Database
            self.db_manager = DatabaseManager()
            self.db_connected = True
            logger.info("Connected to database successfully")

        except Exception as e:
            self.connected = False
            self.db_connected = False
            logger.error(f"Failed to connect: {e}")
            raise

    def disconnect(self):
        """Disconnect from Kafka and Database"""
        if self.producer:
            self.producer.close()
            self.connected = False
            logger.info("Disconnected from Kafka")

        if self.db_manager:
            self.db_manager.close()
            self.db_connected = False
            logger.info("Disconnected from database")

    def send_event(self, event: Dict[str, Any]) -> bool:
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
producer_manager = ProducerManager()


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


def get_producer_manager() -> ProducerManager:
    """Dependency to get producer manager"""
    return producer_manager


# API Endpoints


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    uptime = time.time() - startup_time

    checks = {
        "kafka_connected": producer_manager.connected,
        "database_connected": producer_manager.db_connected,
        "producer_initialized": producer_manager.producer is not None,
        "db_manager_initialized": producer_manager.db_manager is not None,
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
    event_data: Dict[str, Any],
    producer_mgr: ProducerManager = Depends(get_producer_manager),
):
    """Ingest a single event"""

    # with EVENTS_PROCESSING_TIME.time():  # Metrics disabled
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
        # EVENTS_RECEIVED.labels(
        #     event_type="unknown", status="validation_error"
        # ).inc()
        raise HTTPException(status_code=400, detail=f"Validation error: {str(e)}")
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        # EVENTS_RECEIVED.labels(event_type="unknown", status="error").inc()
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@app.post("/events/batch", response_model=BatchEventResponse, tags=["Events"])
async def ingest_batch_events(
    events: List[Dict[str, Any]],
    producer_mgr: ProducerManager = Depends(get_producer_manager),
):
    """Ingest multiple events in batch"""

    if len(events) > 1000:  # Limit batch size
        raise HTTPException(
            status_code=400, detail="Batch size cannot exceed 1000 events"
        )

    processed_count = 0
    failed_count = 0
    failed_events = []

    # with EVENTS_PROCESSING_TIME.time():  # Metrics disabled
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
    producer_mgr: ProducerManager = Depends(get_producer_manager),
):
    """Generate and ingest a sample event (for testing/development)"""

    from faker import Faker
    import random

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
    producer_mgr: ProducerManager = Depends(get_producer_manager),
):
    """Submit an AI request for processing"""

    if not producer_mgr.db_connected:
        raise HTTPException(status_code=503, detail="Database not connected")

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

        # Send to Kafka first
        success = producer_mgr.send_event(event_dict)

        if success:
            # Store in database and get request_id
            request_id = producer_mgr.db_manager.insert_ai_request(event_dict)

            # EVENTS_RECEIVED.labels(event_type="ai_request", status="success").inc()

            # Estimate wait time based on pending requests
            pending_requests = producer_mgr.db_manager.get_pending_ai_requests(
                limit=1000
            )
            estimated_wait = (
                len(pending_requests) * 10
            )  # Rough estimate: 10 seconds per request

            return AIRequestResponse(
                success=True,
                request_id=request_id,
                message="AI request submitted successfully",
                estimated_wait_time_seconds=estimated_wait,
            )
        else:
            raise HTTPException(status_code=500, detail="Failed to submit AI request")

    except Exception as e:
        logger.error(f"Error submitting AI request: {e}")
        # EVENTS_RECEIVED.labels(event_type="ai_request", status="error").inc()
        raise HTTPException(
            status_code=500, detail=f"Failed to submit AI request: {str(e)}"
        )


@app.get("/ai/requests/{request_id}", response_model=AIStatusResponse, tags=["AI"])
async def get_ai_request_status(
    request_id: str, producer_mgr: ProducerManager = Depends(get_producer_manager)
):
    """Get the status of an AI request"""

    if not producer_mgr.db_connected:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        request_data = producer_mgr.db_manager.get_ai_request_by_id(request_id)

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
    producer_mgr: ProducerManager = Depends(get_producer_manager),
):
    """Get AI requests for a specific user"""

    if not producer_mgr.db_connected:
        raise HTTPException(status_code=503, detail="Database not connected")

    if limit > 100:
        limit = 100  # Prevent excessive queries

    try:
        requests_data = producer_mgr.db_manager.get_ai_requests_by_user(user_id, limit)

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
        "database_connected": producer_manager.db_connected,
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
        reload=False,  # Disable reload to prevent Prometheus metric duplication
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
