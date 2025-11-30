"""
User Profile Data Generator
===========================
Generates realistic user/customer profile data.

Creates synthetic user accounts including:
- Personal information
- Demographics
- Account details
- Customer segments

Output: CSV file with 100K+ user profiles
"""

import random
from datetime import datetime, timedelta
from faker import Faker
import pandas as pd
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_USERS = 100_000
OUTPUT_DIR = "../data/raw/users"

# Customer segments
CUSTOMER_SEGMENTS = {
    'vip': 0.05,           # 5% VIP customers
    'loyal': 0.15,         # 15% loyal
    'regular': 0.40,       # 40% regular
    'occasional': 0.30,    # 30% occasional
    'new': 0.10           # 10% new
}

# Account status distribution
ACCOUNT_STATUS = {
    'active': 0.85,
    'inactive': 0.10,
    'suspended': 0.03,
    'closed': 0.02
}


def weighted_choice(choices_dict):
    """Select item based on weighted probability"""
    items = list(choices_dict.keys())
    weights = list(choices_dict.values())
    return random.choices(items, weights=weights, k=1)[0]


def generate_user():
    """
    Generate a single user profile
    
    Returns:
        Dictionary containing user profile data
    """
    # User identifiers
    user_id = f"USER-{random.randint(100000, 999999)}"
    
    # Personal information
    gender = random.choice(['Male', 'Female', 'Other'])
    
    if gender == 'Male':
        first_name = fake.first_name_male()
    elif gender == 'Female':
        first_name = fake.first_name_female()
    else:
        first_name = fake.first_name()
    
    last_name = fake.last_name()
    full_name = f"{first_name} {last_name}"
    
    # Date of birth (18-75 years old)
    age = random.randint(18, 75)
    dob = fake.date_of_birth(minimum_age=age, maximum_age=age)
    
    # Contact information
    email = f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@{fake.free_email_domain()}"
    phone = fake.phone_number()
    
    # Address details
    country = fake.country()
    state = fake.state()
    city = fake.city()
    zipcode = fake.zipcode()
    address = fake.street_address()
    
    # Account details
    account_created = datetime.now() - timedelta(days=random.randint(1, 1825))  # Up to 5 years ago
    last_login = datetime.now() - timedelta(days=random.randint(0, 90))
    
    # Customer behavior metrics
    customer_segment = weighted_choice(CUSTOMER_SEGMENTS)
    
    # Total purchases based on segment
    if customer_segment == 'vip':
        total_purchases = random.randint(50, 200)
        lifetime_value = random.randint(50000, 500000)
    elif customer_segment == 'loyal':
        total_purchases = random.randint(20, 50)
        lifetime_value = random.randint(20000, 50000)
    elif customer_segment == 'regular':
        total_purchases = random.randint(5, 20)
        lifetime_value = random.randint(5000, 20000)
    elif customer_segment == 'occasional':
        total_purchases = random.randint(1, 5)
        lifetime_value = random.randint(500, 5000)
    else:  # new
        total_purchases = 0
        lifetime_value = 0
    
    # Account status
    account_status = weighted_choice(ACCOUNT_STATUS)
    
    # Preferences
    preferred_categories = random.sample(
        ['electronics', 'clothing', 'home', 'sports', 'books', 'beauty', 'toys', 'grocery'],
        k=random.randint(1, 3)
    )
    
    # Marketing preferences
    email_subscribed = random.choice([True, False])
    sms_subscribed = random.choice([True, False])
    
    # Build user record
    user = {
        'user_id': user_id,
        'first_name': first_name,
        'last_name': last_name,
        'full_name': full_name,
        'gender': gender,
        'date_of_birth': dob.strftime('%Y-%m-%d'),
        'age': age,
        'email': email,
        'phone': phone,
        'address': address,
        'city': city,
        'state': state,
        'country': country,
        'zipcode': zipcode,
        'account_created_at': account_created.strftime('%Y-%m-%d %H:%M:%S'),
        'last_login_at': last_login.strftime('%Y-%m-%d %H:%M:%S'),
        'account_status': account_status,
        'customer_segment': customer_segment,
        'total_purchases': total_purchases,
        'lifetime_value': round(lifetime_value, 2),
        'average_order_value': round(lifetime_value / total_purchases, 2) if total_purchases > 0 else 0,
        'preferred_categories': ','.join(preferred_categories),
        'email_subscribed': email_subscribed,
        'sms_subscribed': sms_subscribed,
        'preferred_language': random.choice(['English', 'Spanish', 'French', 'German', 'Hindi']),
        'preferred_currency': random.choice(['USD', 'EUR', 'GBP', 'INR', 'CAD']),
        'is_premium_member': customer_segment in ['vip', 'loyal'],
        'referral_code': f"REF{random.randint(10000, 99999)}",
        'kyc_verified': random.choice([True, False])
    }
    
    return user


def generate_user_data(num_users, output_dir):
    """
    Generate complete user dataset
    
    Args:
        num_users: Total number of users to generate
        output_dir: Directory to save output file
    """
    print(f"Starting user profile generation: {num_users:,} users")
    print(f"Output directory: {output_dir}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate all users
    users = []
    
    for i in range(num_users):
        user = generate_user()
        users.append(user)
        
        if (i + 1) % 10000 == 0:
            print(f"  Progress: {i + 1:,}/{num_users:,} ({(i+1)/num_users*100:.1f}%)")
    
    # Convert to DataFrame
    df = pd.DataFrame(users)
    
    # Save as CSV
    csv_file = os.path.join(output_dir, "users.csv")
    df.to_csv(csv_file, index=False)
    print(f"\n✓ CSV file created: {csv_file}")
    
    # Save as Parquet
    parquet_file = os.path.join(output_dir, "users.parquet")
    df.to_parquet(parquet_file, index=False, compression='snappy')
    print(f"✓ Parquet file created: {parquet_file}")
    
    # Print statistics
    print(f"\n📊 User Data Statistics:")
    print(f"   Total users: {len(df):,}")
    print(f"   Customer segments:")
    print(df['customer_segment'].value_counts())
    print(f"\n   Account status:")
    print(df['account_status'].value_counts())
    print(f"\n   Gender distribution:")
    print(df['gender'].value_counts())
    
    print(f"\n✅ User profile generation complete!")


def generate_sample_data(num_samples=1000):
    """Generate small sample dataset for testing"""
    print("Generating sample user data...")
    
    output_dir = "../data/samples"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate sample users
    sample_users = [generate_user() for _ in range(num_samples)]
    
    # Create DataFrame
    df = pd.DataFrame(sample_users)
    
    # Save as CSV
    csv_file = os.path.join(output_dir, "users_sample.csv")
    df.to_csv(csv_file, index=False)
    print(f"✓ CSV sample created: {csv_file}")
    print(f"  Records: {len(df):,}")
    
    return sample_users


if __name__ == "__main__":
    print("=" * 80)
    print("E-COMMERCE USER PROFILE DATA GENERATOR")
    print("=" * 80)
    print()
    
    # Generate sample data
    sample_data = generate_sample_data(1000)
    
    print("\n" + "=" * 80)
    print("SAMPLE USER PROFILE:")
    print("=" * 80)
    sample_df = pd.DataFrame([sample_data[0]])
    print(sample_df.T)
    
    print("\n" + "=" * 80)
    print("FULL DATASET GENERATION")
    print("=" * 80)
    print()
    
    # Generate full dataset
    generate_user_data(
        num_users=NUM_USERS,
        output_dir=OUTPUT_DIR
    )
    
    print("\n" + "=" * 80)
    print("✅ ALL DONE!")
    print("=" * 80)
