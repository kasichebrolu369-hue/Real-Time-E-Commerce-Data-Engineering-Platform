"""
Kafka Producer - Real-Time Clickstream Event Producer
=====================================================

PURPOSE:
Simulate real-time e-commerce clickstream events and publish to Kafka topic.

FEATURES:
- Generates realistic user behavior patterns
- Simulates traffic spikes and seasonality
- Publishes to Kafka topic in JSON format
- Configurable event rate and burst patterns

USAGE:
python kafka_producer.py --topic clickstream-events --rate 100
"""

import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Kafka and event generation configuration"""
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
    KAFKA_TOPIC = 'clickstream-events'
    
    # Event Generation
    EVENT_RATE = 100  # Events per second (base rate)
    BURST_PROBABILITY = 0.1  # 10% chance of traffic burst
    BURST_MULTIPLIER = 5  # 5x events during burst
    
    # User Simulation
    NUM_ACTIVE_USERS = 10000
    USER_ID_PREFIX = "USER-"
    
    # Event Types and Probabilities
    EVENT_TYPES = {
        'view': 0.60,
        'add_to_cart': 0.20,
        'remove_from_cart': 0.05,
        'purchase': 0.08,
        'exit': 0.07
    }
    
    # Device Types
    DEVICE_TYPES = {
        'mobile': 0.55,
        'desktop': 0.35,
        'tablet': 0.10
    }
    
    # Traffic Sources
    TRAFFIC_SOURCES = [
        'google', 'facebook', 'instagram', 'twitter', 'direct',
        'email', 'referral', 'youtube', 'linkedin', 'bing'
    ]


# ============================================================================
# EVENT GENERATION
# ============================================================================

def weighted_choice(choices_dict):
    """Select item based on weighted probability"""
    items = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def generate_clickstream_event():
    """
    Generate a single clickstream event
    
    Returns:
        Dictionary containing event data
    """
    # Generate identifiers
    user_id = f"{Config.USER_ID_PREFIX}{random.randint(100000, 100000 + Config.NUM_ACTIVE_USERS)}"
    session_id = str(uuid.uuid4())
    event_id = str(uuid.uuid4())
    
    # Event details
    event_type = weighted_choice(Config.EVENT_TYPES)
    device_type = weighted_choice(Config.DEVICE_TYPES)
    product_id = f"PROD-{random.randint(10000, 99999)}"
    
    # Create page URL based on event type
    if event_type == 'view':
        page_url = f"/product/{product_id}"
    elif event_type in ['add_to_cart', 'remove_from_cart']:
        page_url = "/cart"
    elif event_type == 'purchase':
        page_url = "/checkout/success"
    else:
        page_url = random.choice(['/home', '/products', '/search', '/deals'])
    
    # Traffic details
    traffic_source = random.choice(Config.TRAFFIC_SOURCES)
    
    if traffic_source == 'direct':
        referrer = 'none'
    elif traffic_source == 'google':
        referrer = 'https://www.google.com/search'
    else:
        referrer = f'https://www.{traffic_source}.com'
    
    # Build event
    event = {
        'event_id': event_id,
        'user_id': user_id,
        'session_id': session_id,
        'event_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'event_timestamp': int(time.time() * 1000),  # Milliseconds
        'page_url': page_url,
        'product_id': product_id if event_type in ['view', 'add_to_cart', 'purchase'] else None,
        'event_type': event_type,
        'device_type': device_type,
        'traffic_source': traffic_source,
        'referrer': referrer,
        'country': random.choice(['US', 'UK', 'CA', 'AU', 'IN', 'DE', 'FR']),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
        'os': random.choice(['Windows', 'MacOS', 'Linux', 'iOS', 'Android'])
    }
    
    return event


# ============================================================================
# KAFKA PRODUCER
# ============================================================================

class ClickstreamProducer:
    """Kafka producer for clickstream events"""
    
    def __init__(self, bootstrap_servers, topic):
        """
        Initialize Kafka producer
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic name
        """
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='snappy'
        )
        
        self.event_count = 0
        self.error_count = 0
        
        logger.info(f"✓ Kafka Producer initialized")
        logger.info(f"  Bootstrap servers: {bootstrap_servers}")
        logger.info(f"  Topic: {topic}")
    
    def send_event(self, event):
        """
        Send event to Kafka topic
        
        Args:
            event: Event dictionary
        """
        try:
            # Use user_id as partition key for user affinity
            future = self.producer.send(
                self.topic,
                key=event['user_id'],
                value=event
            )
            
            # Async callback
            future.add_callback(self.on_send_success)
            future.add_errback(self.on_send_error)
            
            self.event_count += 1
            
        except Exception as e:
            logger.error(f"Error sending event: {str(e)}")
            self.error_count += 1
    
    def on_send_success(self, record_metadata):
        """Callback for successful send"""
        if self.event_count % 1000 == 0:
            logger.info(f"✓ Sent {self.event_count:,} events | "
                       f"Topic: {record_metadata.topic} | "
                       f"Partition: {record_metadata.partition} | "
                       f"Offset: {record_metadata.offset}")
    
    def on_send_error(self, ex):
        """Callback for send errors"""
        logger.error(f"Error sending event: {str(ex)}")
        self.error_count += 1
    
    def flush(self):
        """Flush pending messages"""
        self.producer.flush()
    
    def close(self):
        """Close producer connection"""
        self.producer.close()
        logger.info(f"✓ Producer closed | Total events: {self.event_count:,} | "
                   f"Errors: {self.error_count}")


# ============================================================================
# MAIN PRODUCER LOOP
# ============================================================================

def simulate_traffic_pattern():
    """
    Simulate realistic traffic patterns with bursts
    
    Returns:
        Number of events to generate in this interval
    """
    # Check for burst
    if random.random() < Config.BURST_PROBABILITY:
        # Traffic burst (e.g., flash sale, viral event)
        return Config.EVENT_RATE * Config.BURST_MULTIPLIER
    
    # Normal traffic with some variance
    variance = random.uniform(0.8, 1.2)
    return int(Config.EVENT_RATE * variance)


def run_producer(bootstrap_servers, topic, duration_seconds=None, event_rate=None):
    """
    Run the clickstream event producer
    
    Args:
        bootstrap_servers: List of Kafka broker addresses
        topic: Kafka topic name
        duration_seconds: How long to run (None = infinite)
        event_rate: Events per second (overrides config)
    """
    # Override config if specified
    if event_rate:
        Config.EVENT_RATE = event_rate
    
    logger.info("="*80)
    logger.info("KAFKA CLICKSTREAM PRODUCER")
    logger.info("="*80)
    logger.info(f"Event Rate: {Config.EVENT_RATE} events/second (base)")
    logger.info(f"Burst Multiplier: {Config.BURST_MULTIPLIER}x")
    logger.info(f"Duration: {'Infinite' if not duration_seconds else f'{duration_seconds}s'}")
    logger.info("="*80)
    
    # Initialize producer
    producer = ClickstreamProducer(bootstrap_servers, topic)
    
    start_time = time.time()
    
    try:
        while True:
            # Check duration
            if duration_seconds and (time.time() - start_time) > duration_seconds:
                logger.info("Duration reached, stopping producer...")
                break
            
            # Simulate traffic pattern
            events_this_second = simulate_traffic_pattern()
            
            # Generate and send events
            for _ in range(events_this_second):
                event = generate_clickstream_event()
                producer.send_event(event)
            
            # Sleep for 1 second
            time.sleep(1)
            
            # Periodic stats
            if producer.event_count % 10000 == 0 and producer.event_count > 0:
                elapsed = time.time() - start_time
                rate = producer.event_count / elapsed
                logger.info(f"📊 Performance | Rate: {rate:.2f} events/sec | "
                           f"Total: {producer.event_count:,} | "
                           f"Errors: {producer.error_count}")
    
    except KeyboardInterrupt:
        logger.info("\n⚠️  Keyboard interrupt received, shutting down...")
    
    except Exception as e:
        logger.error(f"❌ Error in producer loop: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        producer.flush()
        producer.close()
        
        # Final stats
        elapsed = time.time() - start_time
        logger.info("\n" + "="*80)
        logger.info("PRODUCER SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Events Sent: {producer.event_count:,}")
        logger.info(f"Total Errors: {producer.error_count}")
        logger.info(f"Duration: {elapsed:.2f} seconds")
        logger.info(f"Average Rate: {producer.event_count/elapsed:.2f} events/sec")
        logger.info("="*80)


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Kafka Clickstream Event Producer',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (comma-separated)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='clickstream-events',
        help='Kafka topic name'
    )
    
    parser.add_argument(
        '--rate',
        type=int,
        default=100,
        help='Base event rate (events per second)'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='Duration in seconds (default: infinite)'
    )
    
    args = parser.parse_args()
    
    # Parse bootstrap servers
    bootstrap_servers = args.bootstrap_servers.split(',')
    
    # Run producer
    run_producer(
        bootstrap_servers=bootstrap_servers,
        topic=args.topic,
        duration_seconds=args.duration,
        event_rate=args.rate
    )


if __name__ == "__main__":
    main()
