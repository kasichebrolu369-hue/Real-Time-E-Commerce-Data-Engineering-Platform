"""
E-Commerce Recommendation Engine
==================================
Train product recommendation model using collaborative filtering (ALS).
Works with pandas - no Spark cluster required!
"""

import pandas as pd
import numpy as np
from sklearn.decomposition import NMF
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
import glob
import os
import pickle
from datetime import datetime

print("="*80)
print("🤖 E-COMMERCE RECOMMENDATION ENGINE")
print("="*80)
print()

# ============================================================================
# LOAD DATA
# ============================================================================

print("📥 Loading data...")
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
data_dir = os.path.join(base_dir, 'data', 'raw')

# Load transactions
txn_files = glob.glob(os.path.join(data_dir, 'transactions', '**', '*.csv'), recursive=True)
df_transactions = pd.concat([pd.read_csv(f) for f in txn_files], ignore_index=True)
print(f"✓ Loaded {len(df_transactions):,} transactions")

# Filter completed purchases only
df_transactions = df_transactions[df_transactions['status'] == 'completed']
print(f"✓ Filtered to {len(df_transactions):,} completed transactions")

# ============================================================================
# FEATURE ENGINEERING
# ============================================================================

print("\n🔧 Building user-item interaction matrix...")

# Create rating from purchase count (implicit feedback)
user_product_counts = df_transactions.groupby(['user_id', 'product_id']).size().reset_index(name='count')

# Normalize counts to ratings (1-5 scale)
max_count = user_product_counts['count'].max()
user_product_counts['rating'] = 1 + (user_product_counts['count'] / max_count) * 4

# Limit to top N users and products to reduce memory usage
top_n_users = 5000
top_n_products = 2000

# Get most active users
top_users = df_transactions['user_id'].value_counts().head(top_n_users).index
user_product_counts = user_product_counts[user_product_counts['user_id'].isin(top_users)]

# Get most popular products
top_products = df_transactions['product_id'].value_counts().head(top_n_products).index
user_product_counts = user_product_counts[user_product_counts['product_id'].isin(top_products)]

# Create pivot table (user-item matrix)
user_item_matrix = user_product_counts.pivot(index='user_id', columns='product_id', values='rating').fillna(0)

print(f"✓ Matrix size: {user_item_matrix.shape[0]:,} users × {user_item_matrix.shape[1]:,} products")
print(f"✓ Sparsity: {((user_item_matrix == 0).sum().sum() / (user_item_matrix.shape[0] * user_item_matrix.shape[1]) * 100):.2f}%")

# ============================================================================
# TRAIN-TEST SPLIT
# ============================================================================

print("\n📊 Splitting data...")

# Get indices of non-zero ratings
non_zero_mask = user_item_matrix > 0
train_mask = non_zero_mask.copy()
test_mask = pd.DataFrame(False, index=user_item_matrix.index, columns=user_item_matrix.columns)

# Randomly select 20% of ratings for testing
np.random.seed(42)
for user in user_item_matrix.index:
    user_ratings = non_zero_mask.loc[user]
    rated_products = user_ratings[user_ratings].index
    
    if len(rated_products) > 1:
        n_test = max(1, int(len(rated_products) * 0.2))
        test_products = np.random.choice(rated_products, size=n_test, replace=False)
        test_mask.loc[user, test_products] = True
        train_mask.loc[user, test_products] = False

train_matrix = user_item_matrix.where(train_mask, 0)
test_matrix = user_item_matrix.where(test_mask, 0)

print(f"✓ Train ratings: {(train_matrix > 0).sum().sum():,}")
print(f"✓ Test ratings: {(test_matrix > 0).sum().sum():,}")

# ============================================================================
# TRAIN MODEL
# ============================================================================

print("\n🎯 Training recommendation model (NMF - Non-negative Matrix Factorization)...")

# Use NMF (similar to ALS) for collaborative filtering
n_factors = 20  # Latent factors
model = NMF(n_components=n_factors, init='random', random_state=42, max_iter=200)

# Fit model on training data
W = model.fit_transform(train_matrix)  # User factors
H = model.components_  # Item factors

# Reconstruct matrix (predictions)
predictions = np.dot(W, H)
predictions_df = pd.DataFrame(predictions, index=user_item_matrix.index, columns=user_item_matrix.columns)

print(f"✓ Model trained with {n_factors} latent factors")

# ============================================================================
# EVALUATE MODEL
# ============================================================================

print("\n📈 Evaluating model performance...")

# Get test predictions
test_indices = test_mask.values
test_actual = user_item_matrix.values[test_indices]
test_pred = predictions[test_indices]

# Remove zeros
nonzero_mask = test_actual > 0
test_actual_nonzero = test_actual[nonzero_mask]
test_pred_nonzero = test_pred[nonzero_mask]

# Calculate metrics
rmse = np.sqrt(mean_squared_error(test_actual_nonzero, test_pred_nonzero))
mae = np.mean(np.abs(test_actual_nonzero - test_pred_nonzero))

print(f"✓ RMSE: {rmse:.4f}")
print(f"✓ MAE: {mae:.4f}")

# ============================================================================
# GENERATE RECOMMENDATIONS
# ============================================================================

print("\n💡 Generating recommendations for sample users...")

def get_recommendations(user_id, n=10):
    """Get top N product recommendations for a user"""
    if user_id not in predictions_df.index:
        return []
    
    user_predictions = predictions_df.loc[user_id]
    already_rated = user_item_matrix.loc[user_id] > 0
    
    # Filter out already rated products
    candidate_products = user_predictions[~already_rated]
    
    # Get top N
    top_products = candidate_products.nlargest(n)
    
    return list(top_products.index)

# Get recommendations for first 5 users
sample_users = user_item_matrix.index[:5]

for user_id in sample_users:
    recs = get_recommendations(user_id, n=10)
    print(f"\n👤 User: {user_id}")
    print(f"   Top 10 Recommendations: {', '.join(recs[:5])}...")

# ============================================================================
# SAVE MODEL
# ============================================================================

print("\n💾 Saving model...")

models_dir = os.path.join(base_dir, 'models')
os.makedirs(models_dir, exist_ok=True)

model_path = os.path.join(models_dir, 'recommendation_model.pkl')
with open(model_path, 'wb') as f:
    pickle.dump({
        'model': model,
        'user_factors': W,
        'item_factors': H,
        'user_index': list(user_item_matrix.index),
        'product_index': list(user_item_matrix.columns),
        'metrics': {'rmse': rmse, 'mae': mae}
    }, f)

print(f"✓ Model saved to: {model_path}")

# Save recommendations for all users
recs_path = os.path.join(models_dir, 'user_recommendations.csv')
all_recs = []

print("\n📝 Generating recommendations for all users (this may take a minute)...")
for i, user_id in enumerate(user_item_matrix.index):
    if (i + 1) % 1000 == 0:
        print(f"   Progress: {i+1:,}/{len(user_item_matrix.index):,} users")
    
    recs = get_recommendations(user_id, n=10)
    for rank, product_id in enumerate(recs, 1):
        all_recs.append({
            'user_id': user_id,
            'product_id': product_id,
            'rank': rank,
            'predicted_score': predictions_df.loc[user_id, product_id]
        })

df_recs = pd.DataFrame(all_recs)
df_recs.to_csv(recs_path, index=False)
print(f"✓ Recommendations saved to: {recs_path}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("✅ RECOMMENDATION ENGINE COMPLETE!")
print("="*80)
print()
print("📊 MODEL SUMMARY:")
print(f"  • Training Method: Non-negative Matrix Factorization (NMF)")
print(f"  • Latent Factors: {n_factors}")
print(f"  • Total Users: {len(user_item_matrix.index):,}")
print(f"  • Total Products: {len(user_item_matrix.columns):,}")
print(f"  • Training Ratings: {(train_matrix > 0).sum().sum():,}")
print(f"  • Test RMSE: {rmse:.4f}")
print(f"  • Test MAE: {mae:.4f}")
print()
print("📁 OUTPUT FILES:")
print(f"  • Model: {model_path}")
print(f"  • Recommendations: {recs_path}")
print()
print("🎯 NEXT STEPS:")
print("  1. Use recommendations in production applications")
print("  2. A/B test recommendations vs baseline")
print("  3. Monitor click-through rates")
print("  4. Retrain model weekly with new data")
print()
print("="*80)
