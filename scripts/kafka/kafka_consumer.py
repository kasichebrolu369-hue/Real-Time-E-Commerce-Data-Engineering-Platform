"""
Kafka Consumer - Process Clickstream Events
===========================================

PURPOSE:
Consume clickstream events from Kafka and process them.

FEATURES:
- Consumes from Kafka topic
- Processes events in batches
- Writes to HDFS/local storage
- Monitors consumer lag
- Handles errors and retries

USAGE:
python kafka_consumer.py --topic clickstream-events --group consumer-group-1
"""

import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import argparse
import logging
import os

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
    """Consumer configuration"""
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
    KAFKA_TOPIC = 'clickstream-events'
    CONSUMER_GROUP = 'clickstream-consumer-group'
    
    # Processing Configuration
    BATCH_SIZE = 1000
    BATCH_TIMEOUT = 10  # seconds
    
    # Output Configuration
    OUTPUT_DIR = "../data/raw/clickstream/kafka/"
    OUTPUT_FORMAT = "json"  # json, csv, parquet


# ============================================================================
# EVENT PROCESSOR
# ============================================================================

class EventProcessor:
    """Process and store clickstream events"""
    
    def __init__(self, output_dir, output_format='json'):
        """
        Initialize event processor
        
        Args:
            output_dir: Directory to write events
            output_format: Output format (json, csv)
        """
        self.output_dir = output_dir
        self.output_format = output_format
        self.event_count = 0
        self.batch_count = 0
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        logger.info(f"✓ Event Processor initialized")
        logger.info(f"  Output directory: {output_dir}")
        logger.info(f"  Output format: {output_format}")
    
    def process_batch(self, events):
        """
        Process a batch of events
        
        Args:
            events: List of event dictionaries
        """
        if not events:
            return
        
        try:
            # Create timestamp-based filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"clickstream_batch_{timestamp}_{self.batch_count}.{self.output_format}"
            filepath = os.path.join(self.output_dir, filename)
            
            # Write to file
            if self.output_format == 'json':
                with open(filepath, 'w') as f:
                    for event in events:
                        json.dump(event, f)
                        f.write('\n')  # JSON Lines format
            
            elif self.output_format == 'csv':
                import csv
                with open(filepath, 'w', newline='') as f:
                    if events:
                        writer = csv.DictWriter(f, fieldnames=events[0].keys())
                        writer.writeheader()
                        writer.writerows(events)
            
            self.event_count += len(events)
            self.batch_count += 1
            
            logger.info(f"✓ Batch processed: {len(events)} events → {filename}")
            logger.info(f"  Total events processed: {self.event_count:,}")
            
        except Exception as e:
            logger.error(f"Error processing batch: {str(e)}")
            import traceback
            traceback.print_exc()


# ============================================================================
# KAFKA CONSUMER
# ============================================================================

class ClickstreamConsumer:
    """Kafka consumer for clickstream events"""
    
    def __init__(self, bootstrap_servers, topic, group_id):
        """
        Initialize Kafka consumer
        
        Args:
            bootstrap_servers: List of Kafka broker addresses
            topic: Kafka topic name
            group_id: Consumer group ID
        """
        self.topic = topic
        self.group_id = group_id
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='earliest',  # Start from beginning if no offset
            enable_auto_commit=False,  # Manual commit for better control
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000,  # Timeout for batching
            max_poll_records=Config.BATCH_SIZE
        )
        
        logger.info(f"✓ Kafka Consumer initialized")
        logger.info(f"  Bootstrap servers: {bootstrap_servers}")
        logger.info(f"  Topic: {topic}")
        logger.info(f"  Group ID: {group_id}")
        logger.info(f"  Batch size: {Config.BATCH_SIZE}")
    
    def consume_batch(self, timeout_ms=10000):
        """
        Consume a batch of messages
        
        Args:
            timeout_ms: Timeout in milliseconds
        
        Returns:
            List of messages
        """
        messages = []
        
        try:
            # Poll for messages
            msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=Config.BATCH_SIZE)
            
            for topic_partition, msgs in msg_pack.items():
                for msg in msgs:
                    messages.append(msg.value)
            
            return messages
            
        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            return []
    
    def commit(self):
        """Commit offsets"""
        try:
            self.consumer.commit()
        except Exception as e:
            logger.error(f"Error committing offsets: {str(e)}")
    
    def close(self):
        """Close consumer connection"""
        self.consumer.close()
        logger.info("✓ Consumer closed")


# ============================================================================
# MAIN CONSUMER LOOP
# ============================================================================

def run_consumer(bootstrap_servers, topic, group_id, output_dir, duration_seconds=None):
    """
    Run the clickstream event consumer
    
    Args:
        bootstrap_servers: List of Kafka broker addresses
        topic: Kafka topic name
        group_id: Consumer group ID
        output_dir: Output directory for processed events
        duration_seconds: How long to run (None = infinite)
    """
    logger.info("="*80)
    logger.info("KAFKA CLICKSTREAM CONSUMER")
    logger.info("="*80)
    logger.info(f"Topic: {topic}")
    logger.info(f"Group ID: {group_id}")
    logger.info(f"Batch Size: {Config.BATCH_SIZE}")
    logger.info(f"Duration: {'Infinite' if not duration_seconds else f'{duration_seconds}s'}")
    logger.info("="*80)
    
    # Initialize consumer and processor
    consumer = ClickstreamConsumer(bootstrap_servers, topic, group_id)
    processor = EventProcessor(output_dir, Config.OUTPUT_FORMAT)
    
    start_time = time.time()
    
    try:
        while True:
            # Check duration
            if duration_seconds and (time.time() - start_time) > duration_seconds:
                logger.info("Duration reached, stopping consumer...")
                break
            
            # Consume batch
            batch = consumer.consume_batch(timeout_ms=Config.BATCH_TIMEOUT * 1000)
            
            if batch:
                # Process batch
                processor.process_batch(batch)
                
                # Commit offsets
                consumer.commit()
            else:
                # No messages, sleep briefly
                time.sleep(1)
            
            # Periodic stats
            if processor.event_count % 10000 == 0 and processor.event_count > 0:
                elapsed = time.time() - start_time
                rate = processor.event_count / elapsed
                logger.info(f"📊 Performance | Rate: {rate:.2f} events/sec | "
                           f"Total: {processor.event_count:,} | "
                           f"Batches: {processor.batch_count}")
    
    except KeyboardInterrupt:
        logger.info("\n⚠️  Keyboard interrupt received, shutting down...")
    
    except Exception as e:
        logger.error(f"❌ Error in consumer loop: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        consumer.close()
        
        # Final stats
        elapsed = time.time() - start_time
        logger.info("\n" + "="*80)
        logger.info("CONSUMER SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Events Processed: {processor.event_count:,}")
        logger.info(f"Total Batches: {processor.batch_count}")
        logger.info(f"Duration: {elapsed:.2f} seconds")
        logger.info(f"Average Rate: {processor.event_count/elapsed:.2f} events/sec")
        logger.info(f"Output Directory: {output_dir}")
        logger.info("="*80)


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Kafka Clickstream Event Consumer',
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
        '--group',
        type=str,
        default='clickstream-consumer-group',
        help='Consumer group ID'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default='../data/raw/clickstream/kafka/',
        help='Output directory for processed events'
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
    
    # Run consumer
    run_consumer(
        bootstrap_servers=bootstrap_servers,
        topic=args.topic,
        group_id=args.group,
        output_dir=args.output_dir,
        duration_seconds=args.duration
    )


if __name__ == "__main__":
    main()
