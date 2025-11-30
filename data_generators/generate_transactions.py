"""
Transaction Data Generator
==========================
Generates realistic e-commerce transaction/order data.

Creates synthetic transaction records including:
- Order details
- Payment information
- Shipping details
- Product quantities and prices

Output: CSV and Parquet files with 500K+ transactions
"""

import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_TRANSACTIONS = 500_000  # 500K transactions
OUTPUT_DIR = "../data/raw/transactions"
BATCH_SIZE = 50_000

# Payment methods distribution
PAYMENT_METHODS = {
    'credit_card': 0.45,
    'debit_card': 0.25,
    'upi': 0.15,
    'paypal': 0.08,
    'net_banking': 0.05,
    'cod': 0.02  # Cash on Delivery
}

# Transaction status distribution
TRANSACTION_STATUS = {
    'completed': 0.85,
    'pending': 0.08,
    'cancelled': 0.05,
    'refunded': 0.02
}

# Product price ranges by category
PRODUCT_PRICES = {
    'electronics': (500, 50000),
    'clothing': (300, 5000),
    'home': (200, 20000),
    'sports': (400, 15000),
    'books': (100, 1500),
    'beauty': (150, 3000),
    'toys': (200, 8000),
    'grocery': (50, 2000),
    'automotive': (500, 50000),
    'jewelry': (1000, 100000)
}

PRODUCT_CATEGORIES = list(PRODUCT_PRICES.keys())


def weighted_choice(choices_dict):
    """Select item based on weighted probability"""
    items = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def generate_transaction():
    """
    Generate a single transaction record
    
    Returns:
        Dictionary containing transaction data
    """
    # Transaction identifiers
    transaction_id = f"TXN-{uuid.uuid4().hex[:12].upper()}"
    user_id = f"USER-{random.randint(100000, 999999)}"
    product_id = f"PROD-{random.randint(10000, 99999)}"
    
    # Product details
    category = random.choice(PRODUCT_CATEGORIES)
    price_range = PRODUCT_PRICES[category]
    base_price = random.randint(price_range[0], price_range[1])
    
    # Quantity (most orders have 1-3 items)
    quantity = random.choices([1, 2, 3, 4, 5], weights=[0.60, 0.25, 0.10, 0.03, 0.02])[0]
    
    # Calculate total with potential discount
    discount_percent = random.choices([0, 5, 10, 15, 20], weights=[0.50, 0.20, 0.15, 0.10, 0.05])[0]
    subtotal = base_price * quantity
    discount_amount = subtotal * (discount_percent / 100)
    tax_amount = (subtotal - discount_amount) * 0.18  # 18% tax
    shipping_fee = 0 if subtotal > 500 else 50
    total_amount = subtotal - discount_amount + tax_amount + shipping_fee
    
    # Payment and status
    payment_method = weighted_choice(PAYMENT_METHODS)
    status = weighted_choice(TRANSACTION_STATUS)
    
    # Timestamp (last 180 days)
    transaction_time = datetime.now() - timedelta(
        days=random.randint(0, 180),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    # Delivery details
    delivery_pincode = fake.zipcode()
    delivery_city = fake.city()
    delivery_state = fake.state()
    delivery_country = fake.country_code()
    
    # Expected delivery (3-7 days from order)
    expected_delivery = transaction_time + timedelta(days=random.randint(3, 7))
    
    # Actual delivery (for completed orders)
    if status == 'completed':
        actual_delivery = transaction_time + timedelta(days=random.randint(2, 8))
    else:
        actual_delivery = None
    
    # Customer details
    customer_email = fake.email()
    customer_phone = fake.phone_number()
    
    # Build transaction record
    transaction = {
        'transaction_id': transaction_id,
        'user_id': user_id,
        'product_id': product_id,
        'category': category,
        'quantity': quantity,
        'unit_price': round(base_price, 2),
        'subtotal': round(subtotal, 2),
        'discount_percent': discount_percent,
        'discount_amount': round(discount_amount, 2),
        'tax_amount': round(tax_amount, 2),
        'shipping_fee': round(shipping_fee, 2),
        'total_amount': round(total_amount, 2),
        'payment_method': payment_method,
        'status': status,
        'transaction_timestamp': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
        'delivery_pincode': delivery_pincode,
        'delivery_city': delivery_city,
        'delivery_state': delivery_state,
        'delivery_country': delivery_country,
        'expected_delivery_date': expected_delivery.strftime('%Y-%m-%d'),
        'actual_delivery_date': actual_delivery.strftime('%Y-%m-%d') if actual_delivery else None,
        'customer_email': customer_email,
        'customer_phone': customer_phone,
        'order_source': random.choice(['web', 'mobile_app', 'mobile_web']),
        'is_first_purchase': random.choice([True, False]),
        'coupon_code': f"COUP{random.randint(1000, 9999)}" if discount_percent > 0 else None
    }
    
    return transaction


def generate_transaction_data(num_transactions, output_dir, batch_size=50_000):
    """
    Generate complete transaction dataset
    
    Args:
        num_transactions: Total number of transactions to generate
        output_dir: Directory to save output files
        batch_size: Number of records per batch
    """
    print(f"Starting transaction generation: {num_transactions:,} records")
    print(f"Output directory: {output_dir}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    total_generated = 0
    batch_num = 0
    
    while total_generated < num_transactions:
        # Generate transactions for this batch
        batch_transactions = []
        
        records_to_generate = min(batch_size, num_transactions - total_generated)
        
        for _ in range(records_to_generate):
            transaction = generate_transaction()
            batch_transactions.append(transaction)
        
        total_generated += len(batch_transactions)
        
        # Convert to DataFrame
        df = pd.DataFrame(batch_transactions)
        
        # Create date-based folder structure
        sample_date = datetime.now() - timedelta(days=random.randint(0, 30))
        date_path = sample_date.strftime('%Y/%m/%d')
        full_output_dir = os.path.join(output_dir, date_path)
        os.makedirs(full_output_dir, exist_ok=True)
        
        # Save as both CSV and Parquet
        csv_file = os.path.join(full_output_dir, f"transactions_batch_{batch_num}.csv")
        parquet_file = os.path.join(full_output_dir, f"transactions_batch_{batch_num}.parquet")
        
        df.to_csv(csv_file, index=False)
        df.to_parquet(parquet_file, index=False, compression='snappy')
        
        batch_num += 1
        print(f"✓ Batch {batch_num} completed: {len(batch_transactions):,} records")
        print(f"  CSV: {csv_file}")
        print(f"  Parquet: {parquet_file}")
        print(f"  Total progress: {total_generated:,}/{num_transactions:,} ({total_generated/num_transactions*100:.1f}%)")
    
    print(f"\n✅ Transaction generation complete!")
    print(f"   Total records: {total_generated:,}")
    print(f"   Total batches: {batch_num}")


def generate_sample_data(num_samples=1000):
    """Generate small sample dataset for testing"""
    print("Generating sample transaction data...")
    
    output_dir = "../data/samples"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate sample transactions
    sample_transactions = [generate_transaction() for _ in range(num_samples)]
    
    # Create DataFrame
    df = pd.DataFrame(sample_transactions)
    
    # Save as CSV
    csv_file = os.path.join(output_dir, "transactions_sample.csv")
    df.to_csv(csv_file, index=False)
    print(f"✓ CSV sample created: {csv_file}")
    
    # Save as Parquet
    parquet_file = os.path.join(output_dir, "transactions_sample.parquet")
    df.to_parquet(parquet_file, index=False, compression='snappy')
    print(f"✓ Parquet sample created: {parquet_file}")
    
    # Print statistics
    print(f"\n📊 Sample Data Statistics:")
    print(f"   Total transactions: {len(df):,}")
    print(f"   Total revenue: ${df['total_amount'].sum():,.2f}")
    print(f"   Average order value: ${df['total_amount'].mean():.2f}")
    print(f"   Status breakdown:")
    print(df['status'].value_counts())
    
    return sample_transactions


if __name__ == "__main__":
    print("=" * 80)
    print("E-COMMERCE TRANSACTION DATA GENERATOR")
    print("=" * 80)
    print()
    
    # Generate sample data
    sample_data = generate_sample_data(1000)
    
    print("\n" + "=" * 80)
    print("SAMPLE TRANSACTION:")
    print("=" * 80)
    sample_df = pd.DataFrame([sample_data[0]])
    print(sample_df.T)
    
    print("\n" + "=" * 80)
    print("FULL DATASET GENERATION")
    print("=" * 80)
    print()
    
    # Generate full dataset
    generate_transaction_data(
        num_transactions=NUM_TRANSACTIONS,
        output_dir=OUTPUT_DIR,
        batch_size=BATCH_SIZE
    )
    
    print("\n" + "=" * 80)
    print("✅ ALL DONE!")
    print("=" * 80)
