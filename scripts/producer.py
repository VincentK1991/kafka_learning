#!/usr/bin/env python3
"""
Legacy Kafka Producer - CLI script for generating events (moved from root)
"""

import json
import logging
import time
import random
from datetime import datetime
from typing import Dict, Any
from kafka import KafkaProducer
from faker import Faker
import sys
sys.path.append('..')
from shared.config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UserEventProducer:
    """Kafka producer for generating and sending user events"""
    
    def __init__(self):
        self.fake = Faker()
        self.producer = KafkaProducer(
            bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        self.topic = Config.KAFKA_TOPIC_NAME
        
    def generate_user_event(self) -> Dict[str, Any]:
        """Generate a realistic user event"""
        event_types = ['login', 'logout', 'page_view', 'purchase', 'signup', 'profile_update']
        
        event = {
            'event_id': self.fake.uuid4(),
            'user_id': random.randint(1000, 9999),
            'event_type': random.choice(event_types),
            'timestamp': datetime.now().isoformat(),
            'user_agent': self.fake.user_agent(),
            'ip_address': self.fake.ipv4(),
            'session_id': self.fake.uuid4(),
            'properties': {}
        }
        
        # Add event-specific properties
        if event['event_type'] == 'purchase':
            event['properties'] = {
                'product_id': random.randint(1, 1000),
                'amount': round(random.uniform(10.0, 500.0), 2),
                'currency': 'USD',
                'category': random.choice(['electronics', 'clothing', 'books', 'home'])
            }
        elif event['event_type'] == 'page_view':
            event['properties'] = {
                'page_url': self.fake.url(),
                'referrer': self.fake.url(),
                'page_title': self.fake.sentence()
            }
        elif event['event_type'] == 'signup':
            event['properties'] = {
                'email': self.fake.email(),
                'name': self.fake.name(),
                'age': random.randint(18, 80),
                'country': self.fake.country()
            }
        
        return event
    
    def send_event(self, event: Dict[str, Any]) -> None:
        """Send an event to Kafka"""
        try:
            # Use user_id as the key for partitioning
            key = str(event['user_id'])
            
            future = self.producer.send(
                self.topic,
                key=key,
                value=event
            )
            
            # Wait for the message to be sent
            record_metadata = future.get(timeout=10)
            
            logger.info(
                f"Sent event {event['event_id']} to topic {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            
        except Exception as e:
            logger.error(f"Failed to send event {event['event_id']}: {e}")
    
    def produce_events(self, num_events: int = 100, delay: float = 1.0) -> None:
        """Produce a specified number of events with delay"""
        logger.info(f"Starting to produce {num_events} events to topic '{self.topic}'")
        
        try:
            for i in range(num_events):
                event = self.generate_user_event()
                self.send_event(event)
                
                if delay > 0:
                    time.sleep(delay)
                    
                if (i + 1) % 10 == 0:
                    logger.info(f"Produced {i + 1} events")
                    
        except KeyboardInterrupt:
            logger.info("Producer interrupted by user")
        except Exception as e:
            logger.error(f"Producer error: {e}")
        finally:
            self.producer.close()
            logger.info("Producer closed")

def main():
    """Main function to run the producer"""
    producer = UserEventProducer()
    
    # Generate events continuously with 1 second delay
    # You can modify these parameters as needed
    producer.produce_events(num_events=1000, delay=1.0)

if __name__ == "__main__":
    main() 