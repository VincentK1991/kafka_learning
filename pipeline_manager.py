#!/usr/bin/env python3
"""
Pipeline Manager - Utilities for managing the Kafka data pipeline
"""

import json
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import subprocess
import time
from typing import Dict, Any, List
from config import Config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineManager:
    """Manager class for the Kafka data pipeline"""
    
    def __init__(self):
        self.postgres_config = Config.get_postgres_config()
    
    def check_kafka_topics(self):
        """Check available Kafka topics"""
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka import KafkaConsumer
            
            # List topics
            consumer = KafkaConsumer(bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS)
            topics = consumer.topics()
            consumer.close()
            
            print(f"Available Kafka topics: {list(topics)}")
            
        except Exception as e:
            logger.error(f"Failed to check Kafka topics: {e}")
    
    def create_kafka_topic(self, topic_name: str = None, num_partitions: int = 3, replication_factor: int = 1):
        """Create Kafka topic if it doesn't exist"""
        from kafka.admin import KafkaAdminClient, NewTopic
        
        if not topic_name:
            topic_name = Config.KAFKA_TOPIC_NAME
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                client_id='pipeline_manager'
            )
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            admin_client.create_topics([topic])
            print(f"Created topic: {topic_name}")
            
        except Exception as e:
            logger.info(f"Topic {topic_name} may already exist: {e}")
    
    def check_database_connection(self):
        """Check PostgreSQL database connection and show table stats"""
        try:
            connection = psycopg2.connect(**self.postgres_config)
            
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if tables exist
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name IN ('user_events', 'transformed_events')
                """)
                tables = [row['table_name'] for row in cursor.fetchall()]
                
                print(f"Database connection: OK")
                print(f"Tables found: {tables}")
                
                # Show table stats if they exist
                if 'user_events' in tables:
                    cursor.execute("SELECT COUNT(*) as count FROM user_events")
                    count = cursor.fetchone()['count']
                    print(f"Raw events in database: {count}")
                
                if 'transformed_events' in tables:
                    cursor.execute("SELECT COUNT(*) as count FROM transformed_events")
                    count = cursor.fetchone()['count']
                    print(f"Transformed events in database: {count}")
            
            connection.close()
            
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            print("Make sure PostgreSQL is running on localhost:5432 and the database exists")
    
    def show_sample_data(self, table_name: str = 'transformed_events', limit: int = 5):
        """Show sample data from specified table"""
        try:
            connection = psycopg2.connect(**self.postgres_config)
            
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY processed_at DESC LIMIT %s", (limit,))
                rows = cursor.fetchall()
                
                print(f"\nSample data from {table_name} (latest {limit} records):")
                print("-" * 80)
                
                for row in rows:
                    print(json.dumps(dict(row), indent=2, default=str))
                    print("-" * 40)
            
            connection.close()
            
        except Exception as e:
            logger.error(f"Failed to fetch sample data: {e}")
    
    def get_analytics_summary(self):
        """Generate analytics summary from transformed data"""
        try:
            connection = psycopg2.connect(**self.postgres_config)
            
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                # Event type distribution
                cursor.execute("""
                    SELECT event_type, COUNT(*) as count, 
                           ROUND(AVG(revenue), 2) as avg_revenue
                    FROM transformed_events 
                    GROUP BY event_type 
                    ORDER BY count DESC
                """)
                event_stats = cursor.fetchall()
                
                # Hourly distribution
                cursor.execute("""
                    SELECT event_hour, COUNT(*) as count
                    FROM transformed_events 
                    GROUP BY event_hour 
                    ORDER BY event_hour
                """)
                hourly_stats = cursor.fetchall()
                
                # Weekend vs weekday
                cursor.execute("""
                    SELECT is_weekend, COUNT(*) as count,
                           ROUND(SUM(revenue), 2) as total_revenue
                    FROM transformed_events 
                    GROUP BY is_weekend
                """)
                weekend_stats = cursor.fetchall()
                
                print("\n=== ANALYTICS SUMMARY ===")
                
                print("\nEvent Type Distribution:")
                for stat in event_stats:
                    print(f"  {stat['event_type']}: {stat['count']} events, avg revenue: ${stat['avg_revenue'] or 0}")
                
                print("\nHourly Distribution (top 5):")
                for stat in hourly_stats[:5]:
                    print(f"  Hour {stat['event_hour']}: {stat['count']} events")
                
                print("\nWeekend vs Weekday:")
                for stat in weekend_stats:
                    day_type = "Weekend" if stat['is_weekend'] else "Weekday"
                    print(f"  {day_type}: {stat['count']} events, total revenue: ${stat['total_revenue'] or 0}")
            
            connection.close()
            
        except Exception as e:
            logger.error(f"Failed to generate analytics: {e}")
    
    def setup_database(self):
        """Setup database schema"""
        try:
            from consumer import DatabaseManager
            db_manager = DatabaseManager()
            print("Database setup completed successfully!")
            db_manager.close()
        except Exception as e:
            logger.error(f"Database setup failed: {e}")

def main():
    """Main function with CLI interface"""
    import sys
    
    manager = PipelineManager()
    
    if len(sys.argv) < 2:
        print("""
Usage: python pipeline_manager.py <command>

Commands:
  check-kafka     - Check Kafka connection and topics
  check-db        - Check database connection and stats
  setup-db        - Setup database tables
  create-topic    - Create Kafka topic
  sample-data     - Show sample data from database
  analytics       - Show analytics summary
  status          - Show overall pipeline status
        """)
        return
    
    command = sys.argv[1]
    
    if command == "check-kafka":
        manager.check_kafka_topics()
    elif command == "check-db":
        manager.check_database_connection()
    elif command == "setup-db":
        manager.setup_database()
    elif command == "create-topic":
        manager.create_kafka_topic()
    elif command == "sample-data":
        table = sys.argv[2] if len(sys.argv) > 2 else 'transformed_events'
        manager.show_sample_data(table)
    elif command == "analytics":
        manager.get_analytics_summary()
    elif command == "status":
        print("=== PIPELINE STATUS ===")
        manager.check_kafka_topics()
        print()
        manager.check_database_connection()
        print()
        manager.get_analytics_summary()
    else:
        print(f"Unknown command: {command}")

if __name__ == "__main__":
    main() 