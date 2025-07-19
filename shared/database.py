#!/usr/bin/env python3
"""
Async Database Connector with connection pooling and dependency injection
"""

import json
import logging
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import asyncpg

from .config import Config

logger = logging.getLogger(__name__)


class AsyncDatabaseConnector:
    """Async database connector with connection pooling and automatic serialization"""

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    async def execute_query(
        self,
        query: str,
        *args,
        fetch_mode: str = "none",  # "none", "one", "all"
    ) -> list[dict[str, Any]] | None:
        """Execute a query with automatic JSON serialization"""
        async with self.pool.acquire() as connection:
            try:
                if fetch_mode == "none":
                    await connection.execute(query, *args)
                    return None
                elif fetch_mode == "one":
                    row = await connection.fetchrow(query, *args)
                    return dict(row) if row else None
                elif fetch_mode == "all":
                    rows = await connection.fetch(query, *args)
                    return [dict(row) for row in rows]
                else:
                    raise ValueError(f"Invalid fetch_mode: {fetch_mode}")
            except Exception as e:
                logger.error(f"Database query failed: {query[:100]}... Error: {e}")
                raise

    async def insert_event(self, event: dict[str, Any]) -> None:
        """Insert raw event into user_events table with automatic JSON serialization"""
        query = """
        INSERT INTO user_events (
            event_id, user_id, event_type, timestamp, user_agent, 
            ip_address, session_id, properties
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (event_id) DO NOTHING
        """

        # Serialize properties to JSON if it's a dict
        properties_json = (
            json.dumps(event.get("properties", {})) if event.get("properties") else None
        )

        # Parse timestamp string to datetime object if it's a string
        timestamp = event["timestamp"]
        if isinstance(timestamp, str):
            # Handle ISO format timestamps
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        await self.execute_query(
            query,
            event["event_id"],
            event["user_id"],
            event["event_type"],
            timestamp,  # Now properly converted to datetime
            event.get("user_agent"),
            event.get("ip_address"),
            event.get("session_id"),
            properties_json,
        )

        logger.info(f"Inserted event {event['event_id']} of type {event['event_type']}")

    async def insert_transformed_event(self, transformed_event: dict[str, Any]) -> None:
        """Insert transformed event into transformed_events table"""
        query = """
        INSERT INTO transformed_events (
            event_id, user_id, event_type, event_date, event_hour, 
            is_weekend, revenue, country, category
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (event_id) DO NOTHING
        """

        await self.execute_query(
            query,
            transformed_event["event_id"],
            transformed_event["user_id"],
            transformed_event["event_type"],
            transformed_event["event_date"],
            transformed_event["event_hour"],
            transformed_event["is_weekend"],
            transformed_event["revenue"],
            transformed_event.get("country"),
            transformed_event.get("category"),
        )

    async def insert_ai_request(self, event: dict[str, Any]) -> str:
        """Insert AI request and return request_id"""
        request_id = str(uuid.uuid4())
        properties = event.get("properties", {})

        query = """
        INSERT INTO ai_requests (
            request_id, event_id, user_id, question, context, model, 
            max_tokens, temperature, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
        """

        await self.execute_query(
            query,
            request_id,
            event["event_id"],
            event["user_id"],
            properties["question"],
            properties.get("context"),
            properties.get("model", "gpt-3.5-turbo"),
            properties.get("max_tokens", 500),
            properties.get("temperature", 0.7),
        )

        logger.info(f"Inserted AI request {request_id} for event {event['event_id']}")
        return request_id

    async def get_pending_ai_requests(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get pending AI requests for processing"""
        query = """
        SELECT request_id, event_id, user_id, question, context, model, 
               max_tokens, temperature, created_at
        FROM ai_requests 
        WHERE status = 'pending' 
        ORDER BY created_at ASC 
        LIMIT $1
        """

        return await self.execute_query(query, limit, fetch_mode="all") or []

    async def update_ai_request_status(
        self,
        request_id: str,
        status: str,
        answer: str | None = None,
        error_message: str | None = None,
        processing_time: float | None = None,
    ) -> None:
        """Update AI request status and response"""
        if status == "processing":
            query = """
            UPDATE ai_requests 
            SET status = $1, started_processing_at = CURRENT_TIMESTAMP
            WHERE request_id = $2
            """
            await self.execute_query(query, status, request_id)
        elif status in ["completed", "failed"]:
            query = """
            UPDATE ai_requests 
            SET status = $1, answer = $2, error_message = $3, 
                processing_time_seconds = $4, completed_at = CURRENT_TIMESTAMP
            WHERE request_id = $5
            """
            await self.execute_query(
                query, status, answer, error_message, processing_time, request_id
            )
        else:
            query = "UPDATE ai_requests SET status = $1 WHERE request_id = $2"
            await self.execute_query(query, status, request_id)

        logger.info(f"Updated AI request {request_id} status to {status}")

    async def get_ai_request_by_id(self, request_id: str) -> dict[str, Any] | None:
        """Get AI request by request_id"""
        query = """
        SELECT request_id, event_id, user_id, question, context, model, 
               max_tokens, temperature, status, answer, error_message, 
               processing_time_seconds, created_at, started_processing_at, completed_at
        FROM ai_requests 
        WHERE request_id = $1
        """

        return await self.execute_query(query, request_id, fetch_mode="one")

    async def get_ai_requests_by_user(
        self, user_id: int, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Get AI requests for a specific user"""
        query = """
        SELECT request_id, event_id, user_id, question, context, model, 
               max_tokens, temperature, status, answer, error_message, 
               processing_time_seconds, created_at, started_processing_at, completed_at
        FROM ai_requests 
        WHERE user_id = $1 
        ORDER BY created_at DESC 
        LIMIT $2
        """

        return await self.execute_query(query, user_id, limit, fetch_mode="all") or []

    async def get_event_stats(self) -> dict[str, Any]:
        """Get comprehensive event statistics"""
        stats = {}

        # Total events
        total_query = "SELECT COUNT(*) as count FROM user_events"
        total_result = await self.execute_query(total_query, fetch_mode="one")
        stats["total_events"] = total_result["count"] if total_result else 0

        # Events by type
        type_query = """
        SELECT event_type, COUNT(*) as count 
        FROM user_events 
        GROUP BY event_type
        """
        type_results = await self.execute_query(type_query, fetch_mode="all") or []
        stats["events_by_type"] = {
            row["event_type"]: row["count"] for row in type_results
        }

        # Events last hour
        hour_query = """
        SELECT COUNT(*) as count 
        FROM user_events 
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        """
        hour_result = await self.execute_query(hour_query, fetch_mode="one")
        stats["events_last_hour"] = hour_result["count"] if hour_result else 0

        # Events last 24 hours
        day_query = """
        SELECT COUNT(*) as count 
        FROM user_events 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        day_result = await self.execute_query(day_query, fetch_mode="one")
        stats["events_last_24h"] = day_result["count"] if day_result else 0

        # Calculate average events per minute
        avg_events_per_minute = (
            stats["events_last_24h"] / (24 * 60) if stats["events_last_24h"] > 0 else 0
        )
        stats["avg_events_per_minute"] = round(avg_events_per_minute, 2)

        return stats

    async def get_sample_data(
        self, table: str, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Get sample data from specified table"""
        if table not in ["user_events", "transformed_events", "ai_requests"]:
            raise ValueError(f"Invalid table name: {table}")

        query = f"SELECT * FROM {table} ORDER BY created_at DESC LIMIT $1"
        return await self.execute_query(query, limit, fetch_mode="all") or []


# Connection pool management
_connection_pool: asyncpg.Pool | None = None


async def create_connection_pool() -> asyncpg.Pool:
    """Create database connection pool"""
    global _connection_pool

    if _connection_pool is None:
        config = Config.get_postgres_config()

        # Create connection string for asyncpg
        connection_string = (
            f"postgresql://{config['user']}:{config['password']}"
            f"@{config['host']}:{config['port']}/{config['database']}"
        )

        _connection_pool = await asyncpg.create_pool(
            connection_string,
            min_size=5,  # Minimum connections in pool
            max_size=20,  # Maximum connections in pool
            command_timeout=60,  # Query timeout
            server_settings={
                "application_name": "kafka_pipeline_app",
            },
        )

        logger.info("Database connection pool created successfully")

        # Create tables if they don't exist
        await _create_tables_if_needed(_connection_pool)

    return _connection_pool


async def _create_tables_if_needed(pool: asyncpg.Pool) -> None:
    """Create database tables if they don't exist"""

    create_events_table = """
    CREATE TABLE IF NOT EXISTS user_events (
        id SERIAL PRIMARY KEY,
        event_id VARCHAR(255) UNIQUE NOT NULL,
        user_id INTEGER NOT NULL,
        event_type VARCHAR(100) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        user_agent TEXT,
        ip_address INET,
        session_id VARCHAR(255),
        properties JSONB,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_transformed_events_table = """
    CREATE TABLE IF NOT EXISTS transformed_events (
        id SERIAL PRIMARY KEY,
        event_id VARCHAR(255) UNIQUE NOT NULL,
        user_id INTEGER NOT NULL,
        event_type VARCHAR(100) NOT NULL,
        event_date DATE NOT NULL,
        event_hour INTEGER NOT NULL,
        is_weekend BOOLEAN NOT NULL,
        revenue DECIMAL(10,2) DEFAULT 0,
        country VARCHAR(100),
        category VARCHAR(100),
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    create_ai_requests_table = """
    CREATE TABLE IF NOT EXISTS ai_requests (
        id SERIAL PRIMARY KEY,
        request_id VARCHAR(255) UNIQUE NOT NULL,
        event_id VARCHAR(255) NOT NULL,
        user_id INTEGER NOT NULL,
        question TEXT NOT NULL,
        context TEXT,
        model VARCHAR(100) DEFAULT 'gpt-3.5-turbo',
        max_tokens INTEGER DEFAULT 500,
        temperature DECIMAL(3,2) DEFAULT 0.7,
        status VARCHAR(50) DEFAULT 'pending',
        answer TEXT,
        error_message TEXT,
        processing_time_seconds DECIMAL(10,3),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        started_processing_at TIMESTAMP,
        completed_at TIMESTAMP,
        CONSTRAINT ai_requests_status_check CHECK (status IN ('pending', 'processing', 'completed', 'failed'))
    );
    """

    # Create indexes for better performance
    create_indexes = [
        "CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_user_events_event_type ON user_events(event_type);",
        "CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_user_events_created_at ON user_events(created_at);",
        "CREATE INDEX IF NOT EXISTS idx_transformed_events_user_id ON transformed_events(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_transformed_events_event_date ON transformed_events(event_date);",
        "CREATE INDEX IF NOT EXISTS idx_ai_requests_status ON ai_requests(status);",
        "CREATE INDEX IF NOT EXISTS idx_ai_requests_user_id ON ai_requests(user_id);",
        "CREATE INDEX IF NOT EXISTS idx_ai_requests_created_at ON ai_requests(created_at);",
        "CREATE INDEX IF NOT EXISTS idx_ai_requests_request_id ON ai_requests(request_id);",
    ]

    async with pool.acquire() as connection:
        try:
            await connection.execute(create_events_table)
            await connection.execute(create_transformed_events_table)
            await connection.execute(create_ai_requests_table)

            for index_sql in create_indexes:
                await connection.execute(index_sql)

            logger.info("Database tables and indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise


async def close_connection_pool() -> None:
    """Close database connection pool"""
    global _connection_pool

    if _connection_pool:
        await _connection_pool.close()
        _connection_pool = None
        logger.info("Database connection pool closed")


@asynccontextmanager
async def get_database() -> AsyncGenerator[AsyncDatabaseConnector, None]:
    """Async context manager for database connections"""
    pool = await create_connection_pool()
    db = AsyncDatabaseConnector(pool)
    try:
        yield db
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        raise


# FastAPI dependency injection functions
async def get_db_dependency() -> AsyncGenerator[AsyncDatabaseConnector, None]:
    """FastAPI dependency for database injection"""
    async with get_database() as db:
        yield db


# For backward compatibility and direct usage
async def get_db() -> AsyncDatabaseConnector:
    """Get database connector (for non-dependency injection usage)"""
    pool = await create_connection_pool()
    return AsyncDatabaseConnector(pool)
