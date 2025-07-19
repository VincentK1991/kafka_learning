#!/usr/bin/env python3
"""
AI Agent Service - Processes pending AI requests using OpenAI
"""

import asyncio
import contextlib
import json
import logging
import os
import time
from typing import Any

import openai
import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from shared.config import Config
from shared.database import (
    AsyncDatabaseConnector,
    close_connection_pool,
    get_db,
    get_db_dependency,
)
from shared.models import HealthResponse

load_dotenv()

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global variables
app = FastAPI(
    title="AI Agent Service",
    description="Processes AI requests using OpenAI",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

startup_time = time.time()
processing_task = None
processing_running = False


# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AsyncAIAgent:
    """AI Agent for processing requests with async database operations"""

    def __init__(self):
        self.consumer = None
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        self.connected = False
        self.openai_client = None

        # Initialize OpenAI client
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY environment variable not set")
            # Don't raise error to allow service to start without OpenAI
        else:
            self.openai_client = openai.OpenAI(api_key=api_key)

    def connect_kafka(self):
        """Connect to Kafka"""
        try:
            self.consumer = KafkaConsumer(
                Config.KAFKA_AI_REQUESTS_TOPIC,
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                group_id="ai_agent_group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                consumer_timeout_ms=1000,
            )
            self.connected = True
            logger.info(f"Connected to Kafka topic: {Config.KAFKA_AI_REQUESTS_TOPIC}")
        except KafkaError as e:
            self.connected = False
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def disconnect_kafka(self):
        """Disconnect from Kafka"""
        if self.consumer:
            self.consumer.close()
            self.connected = False
            logger.info("Disconnected from Kafka")

    async def process_ai_request(
        self, request_payload: dict[str, Any], db: AsyncDatabaseConnector
    ) -> None:
        """Process a single AI request received from Kafka"""
        request_id = request_payload["request_id"]
        start_time = time.time()

        try:
            # Mark as processing
            await db.update_ai_request_status(request_id, "processing")

            logger.info(
                f"Processing AI request {request_id}:\
                     {request_payload['question'][:100]}..."
            )

            if not self.openai_client:
                raise Exception("OpenAI client not configured")

            # Prepare the prompt
            messages = [
                {
                    "role": "system",
                    "content": "You are a helpful AI assistant.\
                         Provide clear, accurate, and helpful responses.",
                }
            ]

            if request_payload.get("context"):
                messages.append(
                    {
                        "role": "system",
                        "content": f"Additional context: {request_payload['context']}",
                    }
                )

            messages.append({"role": "user", "content": request_payload["question"]})

            # Call OpenAI API
            try:
                response = self.openai_client.chat.completions.create(
                    model=request_payload.get("model", "gpt-3.5-turbo"),
                    messages=messages,
                    max_tokens=request_payload.get("max_tokens", 500),
                    temperature=float(request_payload.get("temperature", 0.7)),
                )

                answer = response.choices[0].message.content
                processing_time = time.time() - start_time

                # Update as completed
                await db.update_ai_request_status(
                    request_id,
                    "completed",
                    answer=answer,
                    processing_time=processing_time,
                )

                self.processed_count += 1

                logger.info(
                    f"Completed AI request {request_id} in {processing_time:.2f}s"
                )

            except openai.OpenAIError as e:
                error_msg = f"OpenAI API error: {str(e)}"
                processing_time = time.time() - start_time

                await db.update_ai_request_status(
                    request_id,
                    "failed",
                    error_message=error_msg,
                    processing_time=processing_time,
                )

                self.error_count += 1

                logger.error(f"OpenAI error for request {request_id}: {e}")

        except Exception as e:
            processing_time = time.time() - start_time
            error_msg = f"Processing error: {str(e)}"

            try:
                await db.update_ai_request_status(
                    request_id,
                    "failed",
                    error_message=error_msg,
                    processing_time=processing_time,
                )
            except Exception as db_error:
                logger.error(
                    f"Failed to update AI request status in database: {db_error}"
                )

            self.error_count += 1

            logger.error(f"Error processing AI request {request_id}: {e}")

    async def start_consuming(self):
        """Start consuming and processing AI requests from Kafka."""
        if not self.connected:
            raise Exception("AI Agent not connected to Kafka")

        self.running = True
        logger.info("AI Agent started consuming messages...")

        try:
            while self.running:
                try:
                    # Poll for messages with timeout
                    messages = self.consumer.poll(timeout_ms=1000)

                    if not messages:
                        await asyncio.sleep(1)  # a brief pause
                        continue

                    db = await get_db()
                    for topic_partition, records in messages.items():
                        for record in records:
                            if not self.running:
                                break

                            logger.info(
                                f"Received AI request: {record.value['request_id']}"
                            )
                            await self.process_ai_request(record.value, db)

                except Exception as e:
                    logger.error(f"Error during consumption: {e}")
                    await asyncio.sleep(5)  # Wait longer on error

        except KeyboardInterrupt:
            logger.info("AI Agent consumption interrupted.")
        finally:
            self.running = False
            logger.info("AI Agent stopped consuming messages.")

    def stop_processing(self):
        """Stop processing AI requests"""
        self.running = False


# Global AI agent
ai_agent = AsyncAIAgent()


async def run_ai_agent():
    """Run AI agent processing loop"""
    global processing_running
    processing_running = True
    try:
        await ai_agent.start_consuming()
    except Exception as e:
        logger.error(f"AI agent error: {e}")
    finally:
        processing_running = False


@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    global processing_task
    logger.info("Starting AI Agent service...")

    try:
        # Connect to Kafka
        ai_agent.connect_kafka()
        # Start processing in background task
        processing_task = asyncio.create_task(run_ai_agent())

    except Exception as e:
        logger.error(f"Failed to start AI agent: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global processing_running
    logger.info("Shutting down AI Agent service...")

    ai_agent.stop_processing()
    ai_agent.disconnect_kafka()

    # Cancel the processing task
    if processing_task:
        processing_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await processing_task

    await close_connection_pool()
    processing_running = False


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
        "database_connected": db_healthy,
        "kafka_connected": ai_agent.connected,
        "openai_configured": ai_agent.openai_client is not None,
        "processing_running": processing_running,
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


@app.get("/status", tags=["Status"])
async def get_status(db: AsyncDatabaseConnector = Depends(get_db_dependency)):
    """Get AI agent status with async database operations"""
    pending_count = 0
    try:
        pending_requests = await db.get_pending_ai_requests(limit=1000)
        pending_count = len(pending_requests)
    except Exception as e:
        logger.error(f"Failed to get pending AI requests count: {e}")

    return {
        "status": "healthy" if processing_running else "stopped",
        "processing_running": processing_running,
        "uptime_seconds": time.time() - startup_time,
        "processed_count": ai_agent.processed_count,
        "error_count": ai_agent.error_count,
        "pending_requests": pending_count,
        "openai_configured": ai_agent.openai_client is not None,
    }


@app.post("/admin/pause", tags=["Admin"])
async def pause_processing():
    """Pause AI request processing"""
    ai_agent.stop_processing()
    return {"message": "AI processing paused", "status": "paused"}


@app.post("/admin/resume", tags=["Admin"])
async def resume_processing():
    """Resume AI request processing"""
    global processing_task, processing_running

    if not processing_running:
        processing_task = asyncio.create_task(run_ai_agent())
        return {"message": "AI processing resumed", "status": "running"}
    else:
        return {"message": "AI processing already running", "status": "running"}


def start_server():
    """Start the FastAPI server"""
    uvicorn.run(
        "services.ai_agent.ai_agent:app",
        host="0.0.0.0",
        port=8003,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
