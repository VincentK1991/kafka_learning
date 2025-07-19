#!/usr/bin/env python3
"""
FastAPI Consumer Server - Pipeline monitoring and management API
"""

import asyncio
import json
import logging
import threading
import time
from datetime import datetime
from typing import Any

import psycopg2
import uvicorn
from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    HTTPException,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor

from shared.config import Config
from shared.consumer import DatabaseManager, DataTransformer
from shared.models import (
    AnalyticsSummary,
    EventStats,
    HealthResponse,
    PipelineStatus,
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
    title="Kafka Event Consumer API",
    description="Pipeline monitoring and management API",
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

startup_time = time.time()
consumer_thread = None
consumer_running = False
websocket_connections: list[WebSocket] = []


# Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ConsumerManager:
    """Manages Kafka consumer and database operations"""

    def __init__(self):
        self.consumer = None
        self.db_manager = None
        self.transformer = DataTransformer()
        self.connected = False
        self.running = False
        self.processed_count = 0
        self.error_count = 0

    def connect(self):
        """Connect to Kafka and database"""
        try:
            # Initialize database connection
            self.db_manager = DatabaseManager()

            # Initialize Kafka consumer
            self.consumer = KafkaConsumer(
                Config.KAFKA_TOPIC_NAME,
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                group_id=Config.KAFKA_CONSUMER_GROUP_ID,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=1000,
                consumer_timeout_ms=1000,  # Timeout for non-blocking consumption
            )

            self.connected = True
            # CONSUMER_HEALTH.set(1)  # Metrics disabled
            logger.info("Connected to Kafka and database successfully")

        except Exception as e:
            self.connected = False
            # CONSUMER_HEALTH.set(0)  # Metrics disabled
            logger.error(f"Failed to connect: {e}")
            raise

    def disconnect(self):
        """Disconnect from Kafka and database"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        if self.db_manager:
            self.db_manager.close()
        self.connected = False
        # CONSUMER_HEALTH.set(0)  # Metrics disabled
        logger.info("Disconnected from Kafka and database")

    def process_event(self, event: dict[str, Any]) -> bool:
        """Process a single event"""
        try:
            # Store raw event
            self.db_manager.insert_event(event)

            # Handle AI requests differently
            if event.get("event_type") == "ai_request":
                # Store AI request in ai_requests table
                request_id = self.db_manager.insert_ai_request(event)
                logger.info(
                    f"Processed AI request {request_id}\
                         for event {event.get('event_id')}"
                )
            else:
                # Transform and store regular events
                transformed_event = self.transformer.transform_event(event)
                self.db_manager.insert_transformed_event(transformed_event)

            self.processed_count += 1

            # Broadcast to WebSocket connections
            asyncio.create_task(self.broadcast_event(event))

            logger.info(
                f"Processed event {event.get('event_id')}\
                     of type {event.get('event_type')}"
            )
            return True

        except Exception as e:
            self.error_count += 1
            logger.error(f"Failed to process event {event.get('event_id')}: {e}")
            return False

    async def broadcast_event(self, event: dict[str, Any]):
        """Broadcast event to WebSocket connections"""
        if websocket_connections:
            message = {
                "type": "event",
                "data": event,
                "timestamp": datetime.now().isoformat(),
            }

            # Send to all connected WebSocket clients
            disconnected = []
            for websocket in websocket_connections:
                try:
                    await websocket.send_json(message)
                except Exception as e:
                    logger.error(f"Failed to send event to WebSocket: {e}")
                    disconnected.append(websocket)

            # Remove disconnected clients
            for ws in disconnected:
                websocket_connections.remove(ws)

    def start_consuming(self):
        """Start consuming events in a separate thread"""
        if not self.connected:
            raise Exception("Consumer not connected")

        self.running = True
        logger.info("Starting event consumption...")

        try:
            while self.running:
                try:
                    # Poll for messages with timeout
                    messages = self.consumer.poll(timeout_ms=1000)

                    for topic_partition, records in messages.items():
                        for record in records:
                            if not self.running:
                                break

                            event = record.value
                            self.process_event(event)

                    # Update consumer lag metric
                    # This is a simplified lag calculation
                    if messages:
                        # CONSUMER_LAG.set(len(messages))  # Metrics disabled
                        pass

                except Exception as e:
                    logger.error(f"Error during consumption: {e}")
                    time.sleep(1)  # Brief pause before retrying

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.running = False
            logger.info("Consumer stopped")

    def stop_consuming(self):
        """Stop consuming events"""
        self.running = False


# Global consumer manager
consumer_manager = ConsumerManager()


def run_consumer():
    """Run consumer in background thread"""
    global consumer_running
    consumer_running = True
    try:
        consumer_manager.start_consuming()
    except Exception as e:
        logger.error(f"Consumer thread error: {e}")
    finally:
        consumer_running = False


@app.on_event("startup")
async def startup_event():
    """Initialize the application"""
    global consumer_thread
    logger.info("Starting Consumer API server...")

    try:
        consumer_manager.connect()

        # Start consumer in background thread
        consumer_thread = threading.Thread(target=run_consumer, daemon=True)
        consumer_thread.start()

    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global consumer_running
    logger.info("Shutting down Consumer API server...")

    consumer_manager.stop_consuming()
    consumer_manager.disconnect()
    consumer_running = False


# API Endpoints


@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """Health check endpoint"""
    uptime = time.time() - startup_time

    checks = {
        "kafka_connected": consumer_manager.connected,
        "database_connected": consumer_manager.db_manager is not None,
        "consumer_running": consumer_running,
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


@app.get("/status", response_model=PipelineStatus, tags=["Pipeline"])
async def get_pipeline_status():
    """Get overall pipeline status"""

    if not consumer_manager.db_manager:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        connection = psycopg2.connect(**Config.get_postgres_config())

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Get events processed in last 24 hours
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM user_events 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
            """)
            events_24h = cursor.fetchone()["count"]

            # Get last event timestamp
            cursor.execute("""
                SELECT MAX(timestamp) as last_timestamp 
                FROM user_events
            """)
            last_event = cursor.fetchone()["last_timestamp"]

        connection.close()

        status = "healthy"
        if not consumer_manager.connected or not consumer_running:
            status = "unhealthy"
        elif consumer_manager.error_count > consumer_manager.processed_count * 0.1:
            status = "degraded"

        return PipelineStatus(
            status=status,
            kafka_connected=consumer_manager.connected,
            database_connected=consumer_manager.db_manager is not None,
            events_processed_24h=events_24h,
            last_event_timestamp=last_event,
            consumer_lag=None,  # Would need more complex calculation
        )

    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")


@app.get("/stats", response_model=EventStats, tags=["Analytics"])
async def get_event_stats():
    """Get event statistics"""

    if not consumer_manager.db_manager:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        connection = psycopg2.connect(**Config.get_postgres_config())

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Total events
            cursor.execute("SELECT COUNT(*) as count FROM user_events")
            total_events = cursor.fetchone()["count"]

            # Events by type
            cursor.execute("""
                SELECT event_type, COUNT(*) as count 
                FROM user_events 
                GROUP BY event_type
            """)
            events_by_type = {
                row["event_type"]: row["count"] for row in cursor.fetchall()
            }

            # Events last hour
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM user_events 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
            """)
            events_last_hour = cursor.fetchone()["count"]

            # Events last 24 hours
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM user_events 
                WHERE created_at >= NOW() - INTERVAL '24 hours'
            """)
            events_last_24h = cursor.fetchone()["count"]

        connection.close()

        # Calculate average events per minute (last 24h)
        avg_events_per_minute = (
            events_last_24h / (24 * 60) if events_last_24h > 0 else 0
        )

        return EventStats(
            total_events=total_events,
            events_by_type=events_by_type,
            events_last_hour=events_last_hour,
            events_last_24h=events_last_24h,
            avg_events_per_minute=round(avg_events_per_minute, 2),
        )

    except Exception as e:
        logger.error(f"Error getting event stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting stats: {str(e)}")


@app.get("/analytics", response_model=AnalyticsSummary, tags=["Analytics"])
async def get_analytics_summary():
    """Get comprehensive analytics summary"""

    if not consumer_manager.db_manager:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        connection = psycopg2.connect(**Config.get_postgres_config())

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            # Event type distribution
            cursor.execute("""
                SELECT event_type, COUNT(*) as count, 
                       ROUND(AVG(revenue), 2) as avg_revenue
                FROM transformed_events 
                GROUP BY event_type 
                ORDER BY count DESC
            """)
            event_distribution = {
                row["event_type"]: {
                    "count": row["count"],
                    "avg_revenue": float(row["avg_revenue"] or 0),
                }
                for row in cursor.fetchall()
            }

            # Hourly distribution
            cursor.execute("""
                SELECT event_hour, COUNT(*) as count
                FROM transformed_events 
                GROUP BY event_hour 
                ORDER BY event_hour
            """)
            hourly_distribution = [
                {"hour": row["event_hour"], "count": row["count"]}
                for row in cursor.fetchall()
            ]

            # Weekend stats
            cursor.execute("""
                SELECT is_weekend, COUNT(*) as count,
                       ROUND(SUM(revenue), 2) as total_revenue
                FROM transformed_events 
                GROUP BY is_weekend
            """)
            weekend_stats = {
                "weekend" if row["is_weekend"] else "weekday": {
                    "count": row["count"],
                    "total_revenue": float(row["total_revenue"] or 0),
                }
                for row in cursor.fetchall()
            }

            # Total revenue
            cursor.execute(
                "SELECT ROUND(SUM(revenue), 2) as total FROM transformed_events"
            )
            total_revenue = float(cursor.fetchone()["total"] or 0)

            # Total events
            cursor.execute("SELECT COUNT(*) as count FROM transformed_events")
            total_events = cursor.fetchone()["count"]

        connection.close()

        return AnalyticsSummary(
            event_distribution=event_distribution,
            hourly_distribution=hourly_distribution,
            weekend_stats=weekend_stats,
            total_revenue=total_revenue,
            total_events=total_events,
        )

    except Exception as e:
        logger.error(f"Error getting analytics: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error getting analytics: {str(e)}"
        )


@app.post("/admin/pause", tags=["Admin"])
async def pause_consumer():
    """Pause event processing"""
    consumer_manager.stop_consuming()
    return {"message": "Consumer paused", "status": "paused"}


@app.post("/admin/resume", tags=["Admin"])
async def resume_consumer():
    """Resume event processing"""
    global consumer_thread, consumer_running

    if not consumer_running:
        consumer_thread = threading.Thread(target=run_consumer, daemon=True)
        consumer_thread.start()
        return {"message": "Consumer resumed", "status": "running"}
    else:
        return {"message": "Consumer already running", "status": "running"}


@app.websocket("/ws/events")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time event streaming"""
    await websocket.accept()
    websocket_connections.append(websocket)

    try:
        # Send initial status
        await websocket.send_json(
            {
                "type": "status",
                "data": {
                    "connected": True,
                    "consumer_running": consumer_running,
                    "processed_count": consumer_manager.processed_count,
                },
            }
        )

        # Keep connection alive
        while True:
            # Send periodic heartbeat
            await asyncio.sleep(30)
            await websocket.send_json(
                {"type": "heartbeat", "timestamp": datetime.now().isoformat()}
            )

    except WebSocketDisconnect:
        websocket_connections.remove(websocket)
        logger.info("WebSocket client disconnected")


@app.get("/sample-data", tags=["Development"])
async def get_sample_data(table: str = "transformed_events", limit: int = 10):
    """Get sample data from database tables"""

    if table not in ["user_events", "transformed_events"]:
        raise HTTPException(status_code=400, detail="Invalid table name")

    if not consumer_manager.db_manager:
        raise HTTPException(status_code=503, detail="Database not connected")

    try:
        connection = psycopg2.connect(**Config.get_postgres_config())

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                f"SELECT * FROM {table} ORDER BY processed_at DESC LIMIT %s", (limit,)
            )
            rows = cursor.fetchall()

        connection.close()

        # Convert to list of dicts with proper JSON serialization
        return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"Error getting sample data: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting data: {str(e)}")


def start_server():
    """Start the FastAPI server"""
    uvicorn.run(
        "services.consumer.consumer_api:app",
        host="0.0.0.0",
        port=8002,
        reload=False,  # Disable reload to prevent Prometheus metric duplication
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
