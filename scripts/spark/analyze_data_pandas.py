"""
Data Analysis using Pandas (No Spark Required)
==============================================

Quick data exploration and analysis using pandas.
Perfect for learning and local development!
"""

import pandas as pd
import glob
import os
from datetime import datetime

# ============================================================================
# CONFIGURATION
# ============================================================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

print("="*80)
print("📊 E-COMMERCE DATA ANALYSIS - PANDAS MODE")
print("="*80)
print(f"Base Directory: {BASE_DIR}")
print(f"Data Directory: {DATA_DIR}")
print("="*80)
print()

# ============================================================================
# LOAD DATA
# ============================================================================

print("📥 LOADING DATA...")
print("-" * 80)

# Load users
users_path = os.path.join(DATA_DIR, 'users', 'users.csv')
df_users = pd.read_csv(users_path)
print(f"✓ Users: {len(df_users):,} records")

# Load products
products_path = os.path.join(DATA_DIR, 'products', 'products.csv')
df_products = pd.read_csv(products_path)
print(f"✓ Products: {len(df_products):,} records")

# Load transactions (all CSV files)
txn_files = glob.glob(os.path.join(DATA_DIR, 'transactions', '**', '*.csv'), recursive=True)
print(f"  Found {len(txn_files)} transaction batch files")
df_transactions = pd.concat([pd.read_csv(f) for f in txn_files], ignore_index=True)
print(f"✓ Transactions: {len(df_transactions):,} records")

# Load clickstream (sample from first file for speed)
click_files = glob.glob(os.path.join(DATA_DIR, 'clickstream', '**', '*.json'), recursive=True)
print(f"  Found {len(click_files)} clickstream batch files (loading sample...)")
df_clickstream = pd.read_json(click_files[0], lines=True)
print(f"✓ Clickstream: {len(df_clickstream):,} events (sample)")

print()

# ============================================================================
# DATA EXPLORATION
# ============================================================================

print("="*80)
print("🔍 DATA EXPLORATION & INSIGHTS")
print("="*80)
print()

# --- USER ANALYSIS ---
print("👥 USER DEMOGRAPHICS")
print("-" * 80)
print(f"Total Users: {len(df_users):,}")
print(f"\nCustomer Segments:")
print(df_users['customer_segment'].value_counts().to_string())
print(f"\nGender Distribution:")
print(df_users['gender'].value_counts().to_string())
print(f"\nTop 10 Cities:")
print(df_users['city'].value_counts().head(10).to_string())
print(f"\nAccount Status:")
print(df_users['account_status'].value_counts().to_string())
print()

# --- PRODUCT ANALYSIS ---
print("🛍️ PRODUCT CATALOG")
print("-" * 80)
print(f"Total Products: {len(df_products):,}")
print(f"In Stock: {df_products['in_stock'].sum():,} ({df_products['in_stock'].mean()*100:.1f}%)")
print(f"Average Price: ${df_products['selling_price'].mean():,.2f}")
print(f"Price Range: ${df_products['selling_price'].min():,.2f} - ${df_products['selling_price'].max():,.2f}")
print(f"\nCategory Distribution:")
print(df_products['category'].value_counts().to_string())
print(f"\nTop 10 Brands:")
print(df_products['brand'].value_counts().head(10).to_string())
print(f"\nFeatured Products: {df_products['is_featured'].sum():,}")
print(f"Bestsellers: {df_products['is_bestseller'].sum():,}")
print()

# --- TRANSACTION ANALYSIS ---
print("💰 TRANSACTION METRICS")
print("-" * 80)
df_transactions['transaction_timestamp'] = pd.to_datetime(df_transactions['transaction_timestamp'])
df_transactions['transaction_date'] = df_transactions['transaction_timestamp'].dt.date

total_revenue = df_transactions['total_amount'].sum()
avg_order = df_transactions['total_amount'].mean()
total_orders = len(df_transactions)

print(f"Total Orders: {total_orders:,}")
print(f"Total Revenue: ${total_revenue:,.2f}")
print(f"Average Order Value: ${avg_order:,.2f}")
print(f"\nOrder Status:")
print(df_transactions['status'].value_counts().to_string())
print(f"\nPayment Methods:")
print(df_transactions['payment_method'].value_counts().to_string())
print(f"\nTop 5 Products by Revenue:")
revenue_by_product = df_transactions.groupby('product_id')['total_amount'].sum().sort_values(ascending=False).head(5)
for product_id, revenue in revenue_by_product.items():
    product_name = df_products[df_products['product_id'] == product_id]['product_name'].values
    if len(product_name) > 0:
        print(f"  {product_id}: ${revenue:,.2f} - {product_name[0][:50]}")
print()

# Daily sales trend
daily_sales = df_transactions.groupby('transaction_date').agg({
    'total_amount': 'sum',
    'transaction_id': 'count'
}).rename(columns={'total_amount': 'revenue', 'transaction_id': 'orders'})
print(f"Daily Sales Statistics:")
print(f"  Average Daily Revenue: ${daily_sales['revenue'].mean():,.2f}")
print(f"  Average Daily Orders: {daily_sales['orders'].mean():,.0f}")
print(f"  Peak Day Revenue: ${daily_sales['revenue'].max():,.2f}")
print(f"  Total Days: {len(daily_sales)}")
print()

# --- CLICKSTREAM ANALYSIS ---
print("🖱️ CLICKSTREAM BEHAVIOR")
print("-" * 80)
print(f"Total Events (sample): {len(df_clickstream):,}")
print(f"\nEvent Types:")
print(df_clickstream['event_type'].value_counts().to_string())
print(f"\nDevice Types:")
print(df_clickstream['device_type'].value_counts().to_string())
print(f"\nTop Traffic Sources:")
print(df_clickstream['traffic_source'].value_counts().head(10).to_string())
print(f"\nTop Browsers:")
print(df_clickstream['browser'].value_counts().head(5).to_string())
print()

# --- CUSTOMER INSIGHTS ---
print("🎯 CUSTOMER INSIGHTS")
print("-" * 80)

# Join transactions with users
df_txn_user = df_transactions.merge(df_users[['user_id', 'customer_segment']], on='user_id', how='left')

print(f"Revenue by Customer Segment:")
segment_revenue = df_txn_user.groupby('customer_segment')['total_amount'].agg(['sum', 'mean', 'count'])
segment_revenue.columns = ['Total Revenue', 'Avg Order Value', 'Num Orders']
segment_revenue['Total Revenue'] = segment_revenue['Total Revenue'].apply(lambda x: f"${x:,.2f}")
segment_revenue['Avg Order Value'] = segment_revenue['Avg Order Value'].apply(lambda x: f"${x:,.2f}")
print(segment_revenue.to_string())
print()

# Category performance
print(f"Revenue by Product Category:")
category_revenue = df_transactions.groupby('category')['total_amount'].sum().sort_values(ascending=False)
for category, revenue in category_revenue.items():
    print(f"  {category:15s}: ${revenue:,.2f}")
print()

# --- CONVERSION METRICS ---
print("📈 CONVERSION METRICS")
print("-" * 80)

# Calculate metrics from clickstream and transactions
unique_visitors = df_clickstream['user_id'].nunique()
unique_buyers = df_transactions['user_id'].nunique()
total_views = len(df_clickstream[df_clickstream['event_type'] == 'view'])
total_purchases = len(df_transactions[df_transactions['status'] == 'completed'])

if unique_visitors > 0:
    conversion_rate = (unique_buyers / unique_visitors) * 100
    print(f"Unique Visitors: {unique_visitors:,}")
    print(f"Unique Buyers: {unique_buyers:,}")
    print(f"Conversion Rate: {conversion_rate:.2f}%")
print(f"Total Product Views: {total_views:,}")
print(f"Total Completed Purchases: {total_purchases:,}")
print()

# ============================================================================
# BUSINESS INTELLIGENCE QUERIES
# ============================================================================

print("="*80)
print("💡 BUSINESS INTELLIGENCE INSIGHTS")
print("="*80)
print()

# Query 1: Top 10 customers by lifetime value
print("🏆 TOP 10 CUSTOMERS BY LIFETIME VALUE")
print("-" * 80)
top_customers = df_users.nlargest(10, 'lifetime_value')[['user_id', 'full_name', 'customer_segment', 'lifetime_value', 'total_purchases']]
for idx, row in top_customers.iterrows():
    print(f"  {row['full_name']:30s} | Segment: {row['customer_segment']:10s} | LTV: ${row['lifetime_value']:>10,} | Orders: {row['total_purchases']:>4}")
print()

# Query 2: Products with low stock
print("⚠️  PRODUCTS WITH LOW STOCK (< 10 units)")
print("-" * 80)
low_stock = df_products[df_products['stock_quantity'] < 10][['product_id', 'product_name', 'category', 'stock_quantity']].head(10)
for idx, row in low_stock.iterrows():
    print(f"  {row['product_id']:12s} | Stock: {row['stock_quantity']:2d} | {row['product_name'][:60]}")
print()

# Query 3: Abandoned carts
print("🛒 ABANDONED CART ANALYSIS")
print("-" * 80)
add_to_cart = df_clickstream[df_clickstream['event_type'] == 'add_to_cart']['user_id'].unique()
purchasers = df_transactions[df_transactions['status'] == 'completed']['user_id'].unique()
abandoned = set(add_to_cart) - set(purchasers)
abandonment_rate = (len(abandoned) / len(add_to_cart) * 100) if len(add_to_cart) > 0 else 0
print(f"Users who added to cart: {len(add_to_cart):,}")
print(f"Users who purchased: {len(purchasers):,}")
print(f"Users with abandoned carts: {len(abandoned):,}")
print(f"Abandonment Rate: {abandonment_rate:.2f}%")
print()

# ============================================================================
# SUMMARY
# ============================================================================

print("="*80)
print("✅ ANALYSIS COMPLETE!")
print("="*80)
print()
print("📊 SUMMARY STATISTICS:")
print(f"  • {len(df_users):,} users across {df_users['customer_segment'].nunique()} customer segments")
print(f"  • {len(df_products):,} products in {df_products['category'].nunique()} categories")
print(f"  • {len(df_transactions):,} transactions totaling ${total_revenue:,.2f}")
print(f"  • {len(df_clickstream):,}+ clickstream events tracked")
print()
print("🎯 KEY INSIGHTS:")
print(f"  • Average Order Value: ${avg_order:,.2f}")
print(f"  • Order Completion Rate: {(df_transactions['status']=='completed').mean()*100:.1f}%")
print(f"  • Most Popular Category: {df_products['category'].value_counts().index[0]}")
print(f"  • Largest Customer Segment: {df_users['customer_segment'].value_counts().index[0]}")
print()
print("🚀 NEXT STEPS:")
print("  1. Train ML models for recommendations")
print("  2. Build predictive models for churn")
print("  3. Create real-time dashboards")
print("  4. Set up Airflow for automation")
print()
print("="*80)
