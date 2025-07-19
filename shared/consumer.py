#!/usr/bin/env python3
"""
Shared consumer components - DatabaseManager and DataTransformer
"""

import json
import logging
from datetime import datetime
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor

from .config import Config

# Setup logging
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages PostgreSQL database connections and operations"""

    def __init__(self):
        self.config = Config.get_postgres_config()
        self.connection = None
        self.connect()
        self.create_tables()

    def connect(self):
        """Establish connection to PostgreSQL"""
        try:
            self.connection = psycopg2.connect(**self.config)
            self.connection.autocommit = True
            logger.info("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def create_tables(self):
        """Create necessary tables if they don't exist"""
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
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            "CREATE INDEX IF NOT EXISTS idx_transformed_events_user_id ON transformed_events(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_transformed_events_event_date ON transformed_events(event_date);",
            "CREATE INDEX IF NOT EXISTS idx_ai_requests_status ON ai_requests(status);",
            "CREATE INDEX IF NOT EXISTS idx_ai_requests_user_id ON ai_requests(user_id);",
            "CREATE INDEX IF NOT EXISTS idx_ai_requests_created_at ON ai_requests(created_at);",
            "CREATE INDEX IF NOT EXISTS idx_ai_requests_request_id ON ai_requests(request_id);",
        ]

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(create_events_table)
                cursor.execute(create_transformed_events_table)
                cursor.execute(create_ai_requests_table)

                for index_sql in create_indexes:
                    cursor.execute(index_sql)

            logger.info("Database tables and indexes created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise

    def insert_event(self, event: dict[str, Any]) -> None:
        """Insert raw event into user_events table"""
        insert_sql = """
        INSERT INTO user_events (
            event_id, user_id, event_type, timestamp, user_agent, 
            ip_address, session_id, properties
        ) VALUES (
            %(event_id)s, %(user_id)s, %(event_type)s, %(timestamp)s, 
            %(user_agent)s, %(ip_address)s, %(session_id)s, %(properties)s
        ) ON CONFLICT (event_id) DO NOTHING;
        """

        try:
            # Prepare event data for database insertion
            event_data = event.copy()

            # Convert properties dict to JSON string for JSONB field
            if "properties" in event_data and isinstance(
                event_data["properties"], dict
            ):
                event_data["properties"] = json.dumps(event_data["properties"])

            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, event_data)
        except Exception as e:
            logger.error(f"Failed to insert event {event.get('event_id')}: {e}")
            raise

    def insert_transformed_event(self, transformed_event: dict[str, Any]) -> None:
        """Insert transformed event into transformed_events table"""
        insert_sql = """
        INSERT INTO transformed_events (
            event_id, user_id, event_type, event_date, event_hour, 
            is_weekend, revenue, country, category
        ) VALUES (
            %(event_id)s, %(user_id)s, %(event_type)s, %(event_date)s, 
            %(event_hour)s, %(is_weekend)s, %(revenue)s, %(country)s, %(category)s
        ) ON CONFLICT (event_id) DO NOTHING;
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(insert_sql, transformed_event)
        except Exception as e:
            logger.error(
                f"Failed to insert transformed event {transformed_event.get('event_id')}: {e}"
            )
            raise

    def insert_ai_request(self, event: dict[str, Any]) -> str:
        """Insert AI request into ai_requests table and return request_id"""
        import uuid

        request_id = str(uuid.uuid4())
        properties = event.get("properties", {})

        insert_sql = """
        INSERT INTO ai_requests (
            request_id, event_id, user_id, question, context, model, 
            max_tokens, temperature, status
        ) VALUES (
            %(request_id)s, %(event_id)s, %(user_id)s, %(question)s, %(context)s, 
            %(model)s, %(max_tokens)s, %(temperature)s, 'pending'
        );
        """

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    insert_sql,
                    {
                        "request_id": request_id,
                        "event_id": event["event_id"],
                        "user_id": event["user_id"],
                        "question": properties["question"],
                        "context": properties.get("context"),
                        "model": properties.get("model", "gpt-3.5-turbo"),
                        "max_tokens": properties.get("max_tokens", 500),
                        "temperature": properties.get("temperature", 0.7),
                    },
                )
            logger.info(
                f"Inserted AI request {request_id} for event {event['event_id']}"
            )
            return request_id
        except Exception as e:
            logger.error(
                f"Failed to insert AI request for event {event.get('event_id')}: {e}"
            )
            raise

    def get_pending_ai_requests(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get pending AI requests for processing"""
        select_sql = """
        SELECT request_id, event_id, user_id, question, context, model, 
               max_tokens, temperature, created_at
        FROM ai_requests 
        WHERE status = 'pending' 
        ORDER BY created_at ASC 
        LIMIT %s
        """

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (limit,))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get pending AI requests: {e}")
            raise

    def update_ai_request_status(
        self,
        request_id: str,
        status: str,
        answer: str = None,
        error_message: str = None,
        processing_time: float = None,
    ) -> None:
        """Update AI request status and response"""
        if status == "processing":
            update_sql = """
            UPDATE ai_requests 
            SET status = %s, started_processing_at = CURRENT_TIMESTAMP
            WHERE request_id = %s
            """
            params = (status, request_id)
        elif status in ["completed", "failed"]:
            update_sql = """
            UPDATE ai_requests 
            SET status = %s, answer = %s, error_message = %s, 
                processing_time_seconds = %s, completed_at = CURRENT_TIMESTAMP
            WHERE request_id = %s
            """
            params = (status, answer, error_message, processing_time, request_id)
        else:
            update_sql = "UPDATE ai_requests SET status = %s WHERE request_id = %s"
            params = (status, request_id)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(update_sql, params)
            logger.info(f"Updated AI request {request_id} status to {status}")
        except Exception as e:
            logger.error(f"Failed to update AI request {request_id}: {e}")
            raise

    def get_ai_request_by_id(self, request_id: str) -> dict[str, Any] | None:
        """Get AI request by request_id"""
        select_sql = """
        SELECT request_id, event_id, user_id, question, context, model, 
               max_tokens, temperature, status, answer, error_message, 
               processing_time_seconds, created_at, started_processing_at, completed_at
        FROM ai_requests 
        WHERE request_id = %s
        """

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (request_id,))
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            logger.error(f"Failed to get AI request {request_id}: {e}")
            raise

    def get_ai_requests_by_user(
        self, user_id: int, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Get AI requests for a specific user"""
        select_sql = """
        SELECT request_id, event_id, user_id, question, context, model, 
               max_tokens, temperature, status, answer, error_message, 
               processing_time_seconds, created_at, started_processing_at, completed_at
        FROM ai_requests 
        WHERE user_id = %s 
        ORDER BY created_at DESC 
        LIMIT %s
        """

        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(select_sql, (user_id, limit))
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Failed to get AI requests for user {user_id}: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")


class DataTransformer:
    """Handles data transformation logic"""

    @staticmethod
    def transform_event(event: dict[str, Any]) -> dict[str, Any]:
        """Transform raw event data for analytics"""
        timestamp = datetime.fromisoformat(event["timestamp"])

        # Extract date and time components
        event_date = timestamp.date()
        event_hour = timestamp.hour
        is_weekend = timestamp.weekday() >= 5  # Saturday = 5, Sunday = 6

        # Calculate revenue for purchase events
        revenue = 0.0
        if event["event_type"] == "purchase" and "amount" in event.get(
            "properties", {}
        ):
            revenue = float(event["properties"]["amount"])

        # Extract country and category
        country = None
        category = None

        properties = event.get("properties", {})
        if "country" in properties:
            country = properties["country"]
        if "category" in properties:
            category = properties["category"]

        transformed = {
            "event_id": event["event_id"],
            "user_id": event["user_id"],
            "event_type": event["event_type"],
            "event_date": event_date,
            "event_hour": event_hour,
            "is_weekend": is_weekend,
            "revenue": revenue,
            "country": country,
            "category": category,
        }

        return transformed
