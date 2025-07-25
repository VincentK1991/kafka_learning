import os
from functools import cache
from typing import Any


class Config:
    """Configuration class for Kafka data pipeline"""

    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME", "user_events")
    KAFKA_AI_REQUESTS_TOPIC = os.getenv("KAFKA_AI_REQUESTS_TOPIC", "ai_requests")
    KAFKA_CONSUMER_GROUP_ID = os.getenv(
        "KAFKA_CONSUMER_GROUP_ID", "data_transformer_group"
    )

    # Indexing Pipeline Topics
    KAFKA_TEXT_INGESTION_TOPIC = os.getenv(
        "KAFKA_TEXT_INGESTION_TOPIC", "text_ingestion"
    )
    KAFKA_EXTRACTION_TOPIC = os.getenv("KAFKA_EXTRACTION_TOPIC", "extraction")
    KAFKA_NORMALIZATION_TOPIC = os.getenv("KAFKA_NORMALIZATION_TOPIC", "normalization")

    # PostgreSQL Configuration
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "kafka_pipeline")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "kafka_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "kafka_password")

    # Neo4j Configuration
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
    NEO4J_PASSWORD = "password123"

    # Application Configuration
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    @classmethod
    def get_postgres_config(cls) -> dict[str, Any]:
        """Return PostgreSQL connection configuration"""
        return {
            "host": cls.POSTGRES_HOST,
            "port": cls.POSTGRES_PORT,
            "database": cls.POSTGRES_DB,
            "user": cls.POSTGRES_USER,
            "password": cls.POSTGRES_PASSWORD,
        }

    @classmethod
    def get_neo4j_config(cls) -> dict[str, Any]:
        """Return Neo4j connection configuration"""
        return {
            "uri": cls.NEO4J_URI,
            "user": cls.NEO4J_USERNAME,
            "password": cls.NEO4J_PASSWORD,
        }

    @classmethod
    def get_kafka_config(cls) -> dict[str, Any]:
        """Return Kafka configuration"""
        return {
            "bootstrap_servers": [cls.KAFKA_BOOTSTRAP_SERVERS],
            "topic": cls.KAFKA_TOPIC_NAME,
            "group_id": cls.KAFKA_CONSUMER_GROUP_ID,
        }


@cache
def get_config() -> Config:
    """Return a Config instance"""
    return Config()
