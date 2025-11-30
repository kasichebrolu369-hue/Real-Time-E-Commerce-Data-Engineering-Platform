"""
Clickstream Data Generator
==========================
Generates realistic e-commerce clickstream data in JSON format.

This script creates synthetic user behavior data including:
- Page views
- Product interactions
- Cart actions
- Purchase events

Output: JSON files with 1M+ records
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import os

# Initialize Faker for realistic data generation
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_RECORDS = 1_000_000  # Generate 1 million records
OUTPUT_DIR = "../data/raw/clickstream"
BATCH_SIZE = 100_000  # Write in batches to manage memory

# Realistic event type distribution (weighted probabilities)
EVENT_TYPES = {
    'view': 0.60,           # 60% views
    'add_to_cart': 0.20,    # 20% add to cart
    'remove_from_cart': 0.05,  # 5% remove from cart
    'purchase': 0.08,       # 8% purchases
    'exit': 0.07           # 7% exits
}

# Device types distribution
DEVICE_TYPES = {
    'mobile': 0.55,
    'desktop': 0.35,
    'tablet': 0.10
}

# Traffic sources
TRAFFIC_SOURCES = [
    'google', 'facebook', 'instagram', 'twitter', 'direct',
    'email', 'referral', 'youtube', 'linkedin', 'bing'
]

# Sample product categories and URLs
PRODUCT_CATEGORIES = [
    'electronics', 'clothing', 'home', 'sports', 'books',
    'beauty', 'toys', 'grocery', 'automotive', 'jewelry'
]

# Realistic page URLs
PAGE_URLS = [
    '/home', '/products', '/cart', '/checkout', '/account',
    '/search', '/category', '/deals', '/wishlist', '/help'
]


def weighted_choice(choices_dict):
    """Select item based on weighted probability"""
    items = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def generate_clickstream_event(event_id, user_id, session_id, base_time):
    """
    Generate a single clickstream event
    
    Args:
        event_id: Unique event identifier
        user_id: User identifier
        session_id: Session identifier
        base_time: Base timestamp for the event
    
    Returns:
        Dictionary containing clickstream event data
    """
    event_type = weighted_choice(EVENT_TYPES)
    device_type = weighted_choice(DEVICE_TYPES)
    
    # Generate realistic product ID
    product_id = f"PROD-{random.randint(10000, 99999)}"
    
    # Create page URL based on event type
    if event_type == 'view':
        page_url = f"/product/{product_id}"
    elif event_type in ['add_to_cart', 'remove_from_cart']:
        page_url = "/cart"
    elif event_type == 'purchase':
        page_url = "/checkout/success"
    else:
        page_url = random.choice(PAGE_URLS)
    
    # Add realistic variation to timestamp (within same session)
    event_time = base_time + timedelta(seconds=random.randint(0, 3600))
    
    # Generate IP address
    ip_address = fake.ipv4()
    
    # Traffic source and referrer
    traffic_source = random.choice(TRAFFIC_SOURCES)
    
    if traffic_source == 'direct':
        referrer = 'none'
    elif traffic_source == 'google':
        referrer = 'https://www.google.com/search'
    elif traffic_source == 'facebook':
        referrer = 'https://www.facebook.com'
    else:
        referrer = f'https://www.{traffic_source}.com'
    
    # Build event record
    event = {
        'event_id': event_id,
        'user_id': user_id,
        'session_id': session_id,
        'event_time': event_time.strftime('%Y-%m-%d %H:%M:%S'),
        'page_url': page_url,
        'product_id': product_id if event_type in ['view', 'add_to_cart', 'purchase'] else None,
        'event_type': event_type,
        'device_type': device_type,
        'ip_address': ip_address,
        'traffic_source': traffic_source,
        'referrer': referrer,
        'user_agent': fake.user_agent(),
        'country': fake.country_code(),
        'city': fake.city(),
        'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']),
        'os': random.choice(['Windows', 'MacOS', 'Linux', 'iOS', 'Android'])
    }
    
    return event


def generate_user_session(num_events=5):
    """
    Generate a complete user session with multiple events
    
    Args:
        num_events: Number of events in this session
    
    Returns:
        List of event dictionaries
    """
    user_id = f"USER-{random.randint(100000, 999999)}"
    session_id = str(uuid.uuid4())
    
    # Random start time within last 90 days
    base_time = datetime.now() - timedelta(days=random.randint(0, 90))
    
    events = []
    for i in range(num_events):
        event_id = str(uuid.uuid4())
        event = generate_clickstream_event(event_id, user_id, session_id, base_time)
        events.append(event)
    
    return events


def generate_clickstream_data(num_records, output_dir, batch_size=100_000):
    """
    Generate complete clickstream dataset
    
    Args:
        num_records: Total number of records to generate
        output_dir: Directory to save output files
        batch_size: Number of records per batch/file
    """
    print(f"Starting clickstream generation: {num_records:,} records")
    print(f"Output directory: {output_dir}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    total_generated = 0
    batch_num = 0
    
    while total_generated < num_records:
        batch_events = []
        
        # Generate events for this batch
        while len(batch_events) < batch_size and total_generated < num_records:
            # Each session has 1-10 events
            session_events = generate_user_session(num_events=random.randint(1, 10))
            batch_events.extend(session_events)
            total_generated += len(session_events)
        
        # Limit to exact batch size
        batch_events = batch_events[:min(batch_size, num_records - (batch_num * batch_size))]
        
        # Create date-based folder structure (YYYY/MM/DD)
        sample_date = datetime.now() - timedelta(days=random.randint(0, 30))
        date_path = sample_date.strftime('%Y/%m/%d')
        full_output_dir = os.path.join(output_dir, date_path)
        os.makedirs(full_output_dir, exist_ok=True)
        
        # Write batch to JSON file
        output_file = os.path.join(full_output_dir, f"clickstream_batch_{batch_num}.json")
        
        with open(output_file, 'w') as f:
            for event in batch_events:
                json.dump(event, f)
                f.write('\n')  # JSON Lines format
        
        batch_num += 1
        print(f"✓ Batch {batch_num} completed: {len(batch_events):,} records -> {output_file}")
        print(f"  Total progress: {total_generated:,}/{num_records:,} ({total_generated/num_records*100:.1f}%)")
    
    print(f"\n✅ Clickstream generation complete!")
    print(f"   Total records: {total_generated:,}")
    print(f"   Total batches: {batch_num}")


def generate_sample_data(num_samples=1000):
    """Generate small sample dataset for testing"""
    print("Generating sample data for preview...")
    
    output_dir = "../data/samples"
    os.makedirs(output_dir, exist_ok=True)
    
    sample_events = []
    for _ in range(num_samples // 5):
        session_events = generate_user_session(num_events=5)
        sample_events.extend(session_events)
    
    output_file = os.path.join(output_dir, "clickstream_sample.json")
    
    with open(output_file, 'w') as f:
        json.dump(sample_events[:num_samples], f, indent=2)
    
    print(f"✓ Sample data created: {output_file}")
    print(f"  Records: {len(sample_events[:num_samples])}")
    
    # Also create CSV version for easy viewing
    df = pd.DataFrame(sample_events[:num_samples])
    csv_file = os.path.join(output_dir, "clickstream_sample.csv")
    df.to_csv(csv_file, index=False)
    print(f"✓ CSV sample created: {csv_file}")
    
    return sample_events[:num_samples]


if __name__ == "__main__":
    print("=" * 80)
    print("E-COMMERCE CLICKSTREAM DATA GENERATOR")
    print("=" * 80)
    print()
    
    # First, generate sample data
    sample_data = generate_sample_data(1000)
    
    print("\n" + "=" * 80)
    print("SAMPLE EVENT:")
    print("=" * 80)
    print(json.dumps(sample_data[0], indent=2))
    
    print("\n" + "=" * 80)
    print("FULL DATASET GENERATION")
    print("=" * 80)
    print()
    
    # Generate full dataset
    generate_clickstream_data(
        num_records=NUM_RECORDS,
        output_dir=OUTPUT_DIR,
        batch_size=BATCH_SIZE
    )
    
    print("\n" + "=" * 80)
    print("✅ ALL DONE!")
    print("=" * 80)
