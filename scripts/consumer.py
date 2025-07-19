#!/usr/bin/env python3
"""
Legacy Kafka Consumer - CLI script for consuming events (moved from root)
"""

import json
import logging
import sys
sys.path.append('..')
from shared.config import Config
from shared.consumer import DatabaseManager, DataTransformer
from kafka import KafkaConsumer

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UserEventConsumer:
    """Kafka consumer for processing user events"""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.transformer = DataTransformer()
        self.consumer = KafkaConsumer(
            Config.KAFKA_TOPIC_NAME,
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=Config.KAFKA_CONSUMER_GROUP_ID,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000
        )
        
    def process_event(self, event) -> None:
        """Process a single event: store raw and transformed data"""
        try:
            # Store raw event
            self.db_manager.insert_event(event)
            
            # Transform and store transformed event
            transformed_event = self.transformer.transform_event(event)
            self.db_manager.insert_transformed_event(transformed_event)
            
            logger.info(f"Processed event {event['event_id']} of type {event['event_type']}")
            
        except Exception as e:
            logger.error(f"Failed to process event {event.get('event_id', 'unknown')}: {e}")
    
    def consume_events(self):
        """Main consumer loop"""
        logger.info(f"Starting consumer for topic '{Config.KAFKA_TOPIC_NAME}'")
        
        try:
            for message in self.consumer:
                event = message.value
                self.process_event(event)
                
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.consumer.close()
            self.db_manager.close()
            logger.info("Consumer closed")

def main():
    """Main function to run the consumer"""
    consumer = UserEventConsumer()
    consumer.consume_events()

if __name__ == "__main__":
    main() 