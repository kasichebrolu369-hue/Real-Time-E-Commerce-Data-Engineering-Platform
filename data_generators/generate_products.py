"""
Product Catalog Data Generator
==============================
Generates realistic product catalog data.

Creates synthetic product listings including:
- Product details
- Pricing information
- Inventory levels
- Ratings and reviews

Output: CSV file with 10K+ products
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
NUM_PRODUCTS = 10_000
OUTPUT_DIR = "../data/raw/products"

# Product categories with subcategories
PRODUCT_CATALOG = {
    'electronics': {
        'subcategories': ['smartphones', 'laptops', 'tablets', 'headphones', 'cameras', 'smartwatches'],
        'brands': ['Apple', 'Samsung', 'Sony', 'LG', 'Dell', 'HP', 'Lenovo', 'OnePlus', 'Xiaomi'],
        'price_range': (5000, 150000)
    },
    'clothing': {
        'subcategories': ['mens_wear', 'womens_wear', 'kids_wear', 'footwear', 'accessories'],
        'brands': ['Nike', 'Adidas', 'Puma', 'Zara', 'H&M', 'Levis', 'Gap', 'Forever21'],
        'price_range': (300, 8000)
    },
    'home': {
        'subcategories': ['furniture', 'kitchen', 'bedding', 'decor', 'storage'],
        'brands': ['IKEA', 'Urban Ladder', 'Pepperfry', 'HomeTown', 'Godrej'],
        'price_range': (500, 50000)
    },
    'sports': {
        'subcategories': ['fitness', 'outdoor', 'cycling', 'yoga', 'team_sports'],
        'brands': ['Nike', 'Adidas', 'Reebok', 'Puma', 'Decathlon', 'Cosco'],
        'price_range': (500, 25000)
    },
    'books': {
        'subcategories': ['fiction', 'non_fiction', 'textbooks', 'comics', 'magazines'],
        'brands': ['Penguin', 'HarperCollins', 'Oxford', 'McGraw Hill', 'Scholastic'],
        'price_range': (100, 2000)
    },
    'beauty': {
        'subcategories': ['skincare', 'makeup', 'haircare', 'fragrance', 'personal_care'],
        'brands': ['Loreal', 'Maybelline', 'Lakme', 'Nivea', 'Dove', 'Garnier'],
        'price_range': (150, 5000)
    },
    'toys': {
        'subcategories': ['action_figures', 'dolls', 'educational', 'games', 'outdoor_toys'],
        'brands': ['LEGO', 'Mattel', 'Hasbro', 'Fisher Price', 'Hot Wheels'],
        'price_range': (200, 15000)
    },
    'grocery': {
        'subcategories': ['staples', 'snacks', 'beverages', 'dairy', 'frozen'],
        'brands': ['Nestle', 'Britannia', 'Parle', 'Amul', 'ITC', 'HUL'],
        'price_range': (50, 2000)
    },
    'automotive': {
        'subcategories': ['parts', 'accessories', 'tools', 'care', 'electronics'],
        'brands': ['Bosch', '3M', 'Michelin', 'Philips', 'Castrol'],
        'price_range': (500, 50000)
    },
    'jewelry': {
        'subcategories': ['rings', 'necklaces', 'earrings', 'bracelets', 'watches'],
        'brands': ['Tanishq', 'Malabar Gold', 'Kalyan', 'PC Jeweller', 'Titan'],
        'price_range': (1000, 500000)
    }
}

# Product conditions
PRODUCT_CONDITIONS = ['new', 'refurbished', 'used']


def generate_product():
    """
    Generate a single product record
    
    Returns:
        Dictionary containing product data
    """
    # Product identifiers
    product_id = f"PROD-{random.randint(10000, 99999)}"
    
    # Select category and subcategory
    category = random.choice(list(PRODUCT_CATALOG.keys()))
    category_info = PRODUCT_CATALOG[category]
    subcategory = random.choice(category_info['subcategories'])
    brand = random.choice(category_info['brands'])
    
    # Generate product name
    product_name = f"{brand} {fake.catch_phrase()} {subcategory.replace('_', ' ').title()}"
    
    # Pricing
    price_range = category_info['price_range']
    cost_price = random.randint(price_range[0], price_range[1])
    
    # Markup percentage (20-100%)
    markup = random.uniform(0.20, 1.00)
    selling_price = round(cost_price * (1 + markup), 2)
    
    # Discount (0-50%)
    discount_percent = random.choices([0, 5, 10, 15, 20, 30, 50], 
                                     weights=[0.30, 0.20, 0.20, 0.15, 0.10, 0.03, 0.02])[0]
    
    discounted_price = round(selling_price * (1 - discount_percent / 100), 2)
    
    # Stock and availability
    stock_quantity = random.choices(
        [0, random.randint(1, 10), random.randint(10, 50), random.randint(50, 500)],
        weights=[0.05, 0.15, 0.30, 0.50]
    )[0]
    
    in_stock = stock_quantity > 0
    
    # Ratings and reviews
    if random.random() < 0.80:  # 80% products have ratings
        avg_rating = round(random.uniform(3.0, 5.0), 1)
        num_ratings = random.randint(10, 5000)
        num_reviews = int(num_ratings * random.uniform(0.1, 0.3))  # 10-30% of ratings have reviews
    else:
        avg_rating = None
        num_ratings = 0
        num_reviews = 0
    
    # Product specifications
    weight = round(random.uniform(0.1, 20.0), 2)  # kg
    dimensions = f"{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(5, 50)} cm"
    
    # Product condition
    condition = random.choices(PRODUCT_CONDITIONS, weights=[0.85, 0.10, 0.05])[0]
    
    # Warranty (in months)
    if category in ['electronics', 'automotive', 'home']:
        warranty_months = random.choice([0, 6, 12, 24, 36])
    else:
        warranty_months = 0
    
    # Product dates
    added_date = datetime.now() - timedelta(days=random.randint(1, 730))  # Up to 2 years ago
    last_updated = added_date + timedelta(days=random.randint(0, 90))
    
    # Product tags (for search)
    tags = [category, subcategory, brand]
    if discount_percent > 0:
        tags.append('on_sale')
    if avg_rating and avg_rating >= 4.5:
        tags.append('bestseller')
    if condition == 'new':
        tags.append('new_arrival')
    
    # Build product record
    product = {
        'product_id': product_id,
        'product_name': product_name,
        'category': category,
        'subcategory': subcategory,
        'brand': brand,
        'cost_price': round(cost_price, 2),
        'selling_price': round(selling_price, 2),
        'discount_percent': discount_percent,
        'discounted_price': discounted_price,
        'currency': 'INR',
        'stock_quantity': stock_quantity,
        'in_stock': in_stock,
        'low_stock_alert': stock_quantity > 0 and stock_quantity < 10,
        'avg_rating': avg_rating,
        'num_ratings': num_ratings,
        'num_reviews': num_reviews,
        'weight_kg': weight,
        'dimensions': dimensions,
        'condition': condition,
        'warranty_months': warranty_months,
        'manufacturer': brand,
        'country_of_origin': fake.country(),
        'product_added_date': added_date.strftime('%Y-%m-%d'),
        'last_updated_date': last_updated.strftime('%Y-%m-%d'),
        'is_featured': random.choice([True, False]),
        'is_bestseller': avg_rating >= 4.5 if avg_rating else False,
        'free_shipping': selling_price > 500,
        'returnable': condition == 'new',
        'return_window_days': 30 if condition == 'new' else 0,
        'tags': ','.join(tags),
        'sku': f"SKU-{random.randint(100000, 999999)}",
        'barcode': fake.ean13(),
        'supplier_id': f"SUP-{random.randint(1000, 9999)}",
        'reorder_level': 10,
        'max_order_quantity': random.choice([5, 10, 20, 50, 100])
    }
    
    return product


def generate_product_data(num_products, output_dir):
    """
    Generate complete product catalog
    
    Args:
        num_products: Total number of products to generate
        output_dir: Directory to save output file
    """
    print(f"Starting product catalog generation: {num_products:,} products")
    print(f"Output directory: {output_dir}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate all products
    products = []
    
    for i in range(num_products):
        product = generate_product()
        products.append(product)
        
        if (i + 1) % 1000 == 0:
            print(f"  Progress: {i + 1:,}/{num_products:,} ({(i+1)/num_products*100:.1f}%)")
    
    # Convert to DataFrame
    df = pd.DataFrame(products)
    
    # Save as CSV
    csv_file = os.path.join(output_dir, "products.csv")
    df.to_csv(csv_file, index=False)
    print(f"\n✓ CSV file created: {csv_file}")
    
    # Save as Parquet
    parquet_file = os.path.join(output_dir, "products.parquet")
    df.to_parquet(parquet_file, index=False, compression='snappy')
    print(f"✓ Parquet file created: {parquet_file}")
    
    # Print statistics
    print(f"\n📊 Product Catalog Statistics:")
    print(f"   Total products: {len(df):,}")
    print(f"   In stock: {df['in_stock'].sum():,} ({df['in_stock'].sum()/len(df)*100:.1f}%)")
    print(f"   Average price: ₹{df['discounted_price'].mean():.2f}")
    print(f"   Price range: ₹{df['discounted_price'].min():.2f} - ₹{df['discounted_price'].max():.2f}")
    print(f"\n   Category distribution:")
    print(df['category'].value_counts())
    print(f"\n   Top brands:")
    print(df['brand'].value_counts().head(10))
    
    print(f"\n✅ Product catalog generation complete!")


def generate_sample_data(num_samples=500):
    """Generate small sample dataset for testing"""
    print("Generating sample product data...")
    
    output_dir = "../data/samples"
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate sample products
    sample_products = [generate_product() for _ in range(num_samples)]
    
    # Create DataFrame
    df = pd.DataFrame(sample_products)
    
    # Save as CSV
    csv_file = os.path.join(output_dir, "products_sample.csv")
    df.to_csv(csv_file, index=False)
    print(f"✓ CSV sample created: {csv_file}")
    print(f"  Records: {len(df):,}")
    
    return sample_products


if __name__ == "__main__":
    print("=" * 80)
    print("E-COMMERCE PRODUCT CATALOG DATA GENERATOR")
    print("=" * 80)
    print()
    
    # Generate sample data
    sample_data = generate_sample_data(500)
    
    print("\n" + "=" * 80)
    print("SAMPLE PRODUCT:")
    print("=" * 80)
    sample_df = pd.DataFrame([sample_data[0]])
    print(sample_df.T)
    
    print("\n" + "=" * 80)
    print("FULL DATASET GENERATION")
    print("=" * 80)
    print()
    
    # Generate full dataset
    generate_product_data(
        num_products=NUM_PRODUCTS,
        output_dir=OUTPUT_DIR
    )
    
    print("\n" + "=" * 80)
    print("✅ ALL DONE!")
    print("=" * 80)
