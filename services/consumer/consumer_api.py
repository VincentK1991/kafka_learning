#!/usr/bin/env python3
"""
FastAPI Consumer Server - Pipeline monitoring and management API
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Any

import uvicorn
from dotenv import load_dotenv
from fastapi import (
    Depends,
    FastAPI,
    HTTPException,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from kafka import KafkaConsumer

from shared.config import Config
from shared.consumer import DataTransformer
from shared.database import (
    AsyncDatabaseConnector,
    close_connection_pool,
    get_db_dependency,
)
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


class AsyncConsumerManager:
    """Manages Kafka consumer and async database operations"""

    def __init__(self):
        self.consumer = None
        self.transformer = DataTransformer()
        self.connected = False
        self.running = False
        self.processed_count = 0
        self.error_count = 0

    def connect(self):
        """Connect to Kafka (database connections managed by pool)"""
        try:
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
            logger.info("Connected to Kafka successfully")

        except Exception as e:
            self.connected = False
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def disconnect(self):
        """Disconnect from Kafka"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        self.connected = False
        logger.info("Disconnected from Kafka")

    async def process_event(
        self, event: dict[str, Any], db: AsyncDatabaseConnector
    ) -> bool:
        """Process a single event with async database operations"""
        try:
            # Store raw event
            await db.insert_event(event)

            # Transform and store all events, including ai_request for logging
            transformed_event = self.transformer.transform_event(event)
            await db.insert_transformed_event(transformed_event)

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

    async def start_consuming(self):
        """Start consuming events with async database operations"""
        if not self.connected:
            raise Exception("Consumer not connected")

        self.running = True
        logger.info("Starting event consumption...")

        # Import here to avoid circular imports
        from shared.database import get_db

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

                            # Get database connection for this batch of events
                            db = await get_db()
                            await self.process_event(event, db)

                except Exception as e:
                    logger.error(f"Error during consumption: {e}")
                    await asyncio.sleep(1)  # Brief pause before retrying

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        finally:
            self.running = False
            logger.info("Consumer stopped")

    def stop_consuming(self):
        """Stop consuming events"""
        self.running = False


# Global consumer manager
consumer_manager = AsyncConsumerManager()


async def run_consumer():
    """Run consumer in background with async operations"""
    global consumer_running
    consumer_running = True
    try:
        await consumer_manager.start_consuming()
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

        # Start consumer in background task
        asyncio.create_task(run_consumer())

    except Exception as e:
        logger.error(f"Failed to start consumer: {e}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global consumer_running
    logger.info("Shutting down Consumer API server...")

    consumer_manager.stop_consuming()
    consumer_manager.disconnect()
    await close_connection_pool()
    consumer_running = False


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
        "kafka_connected": consumer_manager.connected,
        "database_connected": db_healthy,
        "consumer_running": consumer_running,
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


@app.get("/status", response_model=PipelineStatus, tags=["Pipeline"])
async def get_pipeline_status(db: AsyncDatabaseConnector = Depends(get_db_dependency)):
    """Get overall pipeline status with async database operations"""
    try:
        # Get events processed in last 24 hours
        events_24h_result = await db.execute_query(
            """
            SELECT COUNT(*) as count 
            FROM user_events 
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """,
            fetch_mode="one",
        )
        events_24h = events_24h_result["count"] if events_24h_result else 0

        # Get last event timestamp
        last_event_result = await db.execute_query(
            """
            SELECT MAX(timestamp) as last_timestamp 
            FROM user_events
        """,
            fetch_mode="one",
        )
        last_event = last_event_result["last_timestamp"] if last_event_result else None

        status = "healthy"
        if not consumer_manager.connected or not consumer_running:
            status = "unhealthy"
        elif consumer_manager.error_count > consumer_manager.processed_count * 0.1:
            status = "degraded"

        return PipelineStatus(
            status=status,
            kafka_connected=consumer_manager.connected,
            database_connected=True,  # If we got here, DB is connected
            events_processed_24h=events_24h,
            last_event_timestamp=last_event,
            consumer_lag=None,  # Would need more complex calculation
        )

    except Exception as e:
        logger.error(f"Error getting pipeline status: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting status: {str(e)}")


@app.get("/stats", response_model=EventStats, tags=["Analytics"])
async def get_event_stats(db: AsyncDatabaseConnector = Depends(get_db_dependency)):
    """Get event statistics using async database operations"""
    try:
        # Use the optimized stats method from AsyncDatabaseConnector
        stats = await db.get_event_stats()

        return EventStats(
            total_events=stats["total_events"],
            events_by_type=stats["events_by_type"],
            events_last_hour=stats["events_last_hour"],
            events_last_24h=stats["events_last_24h"],
            avg_events_per_minute=stats["avg_events_per_minute"],
        )

    except Exception as e:
        logger.error(f"Error getting event stats: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting stats: {str(e)}")


@app.get("/analytics", response_model=AnalyticsSummary, tags=["Analytics"])
async def get_analytics_summary(
    db: AsyncDatabaseConnector = Depends(get_db_dependency),
):
    """Get comprehensive analytics summary with async operations"""
    try:
        # Event type distribution
        event_dist_results = await db.execute_query(
            """
            SELECT event_type, COUNT(*) as count, 
                   ROUND(AVG(revenue), 2) as avg_revenue
            FROM transformed_events 
            GROUP BY event_type 
            ORDER BY count DESC
        """,
            fetch_mode="all",
        )

        event_distribution = {
            row["event_type"]: {
                "count": row["count"],
                "avg_revenue": float(row["avg_revenue"] or 0),
            }
            for row in (event_dist_results or [])
        }

        # Hourly distribution
        hourly_results = await db.execute_query(
            """
            SELECT event_hour, COUNT(*) as count
            FROM transformed_events 
            GROUP BY event_hour 
            ORDER BY event_hour
        """,
            fetch_mode="all",
        )

        hourly_distribution = [
            {"hour": row["event_hour"], "count": row["count"]}
            for row in (hourly_results or [])
        ]

        # Weekend stats
        weekend_results = await db.execute_query(
            """
            SELECT is_weekend, COUNT(*) as count,
                   ROUND(SUM(revenue), 2) as total_revenue
            FROM transformed_events 
            GROUP BY is_weekend
        """,
            fetch_mode="all",
        )

        weekend_stats = {
            "weekend" if row["is_weekend"] else "weekday": {
                "count": row["count"],
                "total_revenue": float(row["total_revenue"] or 0),
            }
            for row in (weekend_results or [])
        }

        # Total revenue
        revenue_result = await db.execute_query(
            "SELECT ROUND(SUM(revenue), 2) as total FROM transformed_events",
            fetch_mode="one",
        )
        total_revenue = float(revenue_result["total"] or 0) if revenue_result else 0

        # Total events
        events_result = await db.execute_query(
            "SELECT COUNT(*) as count FROM transformed_events", fetch_mode="one"
        )
        total_events = events_result["count"] if events_result else 0

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
    global consumer_running

    if not consumer_running:
        asyncio.create_task(run_consumer())
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
async def get_sample_data(
    table: str = "transformed_events",
    limit: int = 10,
    db: AsyncDatabaseConnector = Depends(get_db_dependency),
):
    """Get sample data from database tables using async operations"""
    try:
        data = await db.get_sample_data(table, limit)
        return {"table": table, "count": len(data), "data": data}

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting sample data: {e}")
        raise HTTPException(status_code=500, detail=f"Error getting data: {str(e)}")


def start_server():
    """Start the FastAPI server"""
    uvicorn.run(
        "services.consumer.consumer_api:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    start_server()
