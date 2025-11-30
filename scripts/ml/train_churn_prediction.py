"""
E-Commerce Customer Churn Prediction
=====================================
Predict which customers are likely to churn (stop purchasing).
Uses Logistic Regression with engineered behavioral features.
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, roc_curve
import glob
import os
import pickle
from datetime import datetime, timedelta
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt

print("="*80)
print("🔮 E-COMMERCE CHURN PREDICTION MODEL")
print("="*80)
print()

# ============================================================================
# LOAD DATA
# ============================================================================

print("📥 Loading data...")
base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
data_dir = os.path.join(base_dir, 'data', 'raw')

# Load users
df_users = pd.read_csv(os.path.join(data_dir, 'users', 'users.csv'))
print(f"✓ Loaded {len(df_users):,} users")

# Load transactions
txn_files = glob.glob(os.path.join(data_dir, 'transactions', '**', '*.csv'), recursive=True)
df_transactions = pd.concat([pd.read_csv(f) for f in txn_files], ignore_index=True)
print(f"✓ Loaded {len(df_transactions):,} transactions")

# Convert timestamp
df_transactions['transaction_timestamp'] = pd.to_datetime(df_transactions['transaction_timestamp'])

# ============================================================================
# DEFINE CHURN (TARGET VARIABLE)
# ============================================================================

print("\n🎯 Defining churn...")

# Get latest date in dataset
latest_date = df_transactions['transaction_timestamp'].max()
churn_threshold_days = 60  # No purchase in last 60 days = churned

# Get last purchase date for each user
user_last_purchase = df_transactions.groupby('user_id')['transaction_timestamp'].max().reset_index()
user_last_purchase.columns = ['user_id', 'last_purchase_date']

# Define churn: no purchase in last 60 days
user_last_purchase['days_since_last_purchase'] = (latest_date - user_last_purchase['last_purchase_date']).dt.days
user_last_purchase['is_churned'] = (user_last_purchase['days_since_last_purchase'] > churn_threshold_days).astype(int)

print(f"✓ Churn threshold: {churn_threshold_days} days")
print(f"✓ Churned users: {user_last_purchase['is_churned'].sum():,} ({user_last_purchase['is_churned'].mean()*100:.1f}%)")
print(f"✓ Active users: {(1-user_last_purchase['is_churned']).sum():,} ({(1-user_last_purchase['is_churned']).mean()*100:.1f}%)")

# ============================================================================
# FEATURE ENGINEERING
# ============================================================================

print("\n🔧 Engineering features...")

# Filter to completed transactions
df_completed = df_transactions[df_transactions['status'] == 'completed'].copy()

# 1. Recency, Frequency, Monetary (RFM) Features
rfm = df_completed.groupby('user_id').agg({
    'transaction_timestamp': ['max', 'min', 'count'],
    'total_amount': ['sum', 'mean', 'std']
}).reset_index()

rfm.columns = ['user_id', 'last_purchase', 'first_purchase', 'frequency', 
               'total_spent', 'avg_order_value', 'std_order_value']

rfm['recency_days'] = (latest_date - rfm['last_purchase']).dt.days
rfm['customer_lifetime_days'] = (rfm['last_purchase'] - rfm['first_purchase']).dt.days + 1
rfm['purchase_rate'] = rfm['frequency'] / (rfm['customer_lifetime_days'] / 30)  # per month
rfm['std_order_value'] = rfm['std_order_value'].fillna(0)

# 2. Category Diversity
category_diversity = df_completed.groupby('user_id')['category'].nunique().reset_index()
category_diversity.columns = ['user_id', 'category_diversity']

# 3. Payment Method Diversity
payment_diversity = df_completed.groupby('user_id')['payment_method'].nunique().reset_index()
payment_diversity.columns = ['user_id', 'payment_diversity']

# 4. Cancellation/Refund Rate
user_status = df_transactions.groupby(['user_id', 'status']).size().unstack(fill_value=0).reset_index()
user_status['total_orders'] = user_status.sum(axis=1, numeric_only=True)
user_status['cancellation_rate'] = (user_status.get('cancelled', 0) / user_status['total_orders']).fillna(0)
user_status['refund_rate'] = (user_status.get('refunded', 0) / user_status['total_orders']).fillna(0)

# 5. Time-based features
time_features = df_completed.groupby('user_id').agg({
    'transaction_timestamp': lambda x: (x.max() - x.min()).days
}).reset_index()
time_features.columns = ['user_id', 'days_between_first_last']

# 6. User demographic features
user_features = df_users[['user_id', 'age', 'customer_segment', 'is_premium_member', 
                           'account_status', 'email_subscribed', 'sms_subscribed']].copy()

# Encode categorical variables
user_features['segment_vip'] = (user_features['customer_segment'] == 'vip').astype(int)
user_features['segment_loyal'] = (user_features['customer_segment'] == 'loyal').astype(int)
user_features['is_active'] = (user_features['account_status'] == 'active').astype(int)
user_features['is_premium'] = user_features['is_premium_member'].astype(int)
user_features['email_sub'] = user_features['email_subscribed'].astype(int)
user_features['sms_sub'] = user_features['sms_subscribed'].astype(int)

# Merge all features
df_features = user_features.merge(rfm[['user_id', 'recency_days', 'frequency', 'total_spent', 
                                         'avg_order_value', 'std_order_value', 'customer_lifetime_days', 
                                         'purchase_rate']], on='user_id', how='left')
df_features = df_features.merge(category_diversity, on='user_id', how='left')
df_features = df_features.merge(payment_diversity, on='user_id', how='left')
df_features = df_features.merge(user_status[['user_id', 'cancellation_rate', 'refund_rate']], on='user_id', how='left')
df_features = df_features.merge(user_last_purchase[['user_id', 'is_churned']], on='user_id', how='left')

# Fill NaN values (users with no transactions)
df_features = df_features.fillna(0)

# Remove users with no transaction history
df_features = df_features[df_features['frequency'] > 0].copy()

print(f"✓ Created {df_features.shape[1]-2:,} features for {len(df_features):,} users")

# ============================================================================
# PREPARE DATA FOR MODELING
# ============================================================================

print("\n📊 Preparing train/test split...")

# Select feature columns
feature_cols = ['age', 'recency_days', 'frequency', 'total_spent', 'avg_order_value', 
                'std_order_value', 'customer_lifetime_days', 'purchase_rate', 
                'category_diversity', 'payment_diversity', 'cancellation_rate', 'refund_rate',
                'segment_vip', 'segment_loyal', 'is_active', 'is_premium', 'email_sub', 'sms_sub']

X = df_features[feature_cols]
y = df_features['is_churned']

# Train/test split (80/20)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Standardize features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print(f"✓ Train set: {len(X_train):,} users")
print(f"✓ Test set: {len(X_test):,} users")
print(f"✓ Train churn rate: {y_train.mean()*100:.1f}%")
print(f"✓ Test churn rate: {y_test.mean()*100:.1f}%")

# ============================================================================
# TRAIN MODELS
# ============================================================================

print("\n🎯 Training models...")

# Model 1: Logistic Regression
print("\n1️⃣ Logistic Regression...")
lr_model = LogisticRegression(random_state=42, max_iter=1000, class_weight='balanced')
lr_model.fit(X_train_scaled, y_train)

lr_pred = lr_model.predict(X_test_scaled)
lr_pred_proba = lr_model.predict_proba(X_test_scaled)[:, 1]
lr_auc = roc_auc_score(y_test, lr_pred_proba)

print(f"✓ Trained - AUC: {lr_auc:.4f}")

# Model 2: Random Forest
print("\n2️⃣ Random Forest...")
rf_model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced', max_depth=10)
rf_model.fit(X_train, y_train)

rf_pred = rf_model.predict(X_test)
rf_pred_proba = rf_model.predict_proba(X_test)[:, 1]
rf_auc = roc_auc_score(y_test, rf_pred_proba)

print(f"✓ Trained - AUC: {rf_auc:.4f}")

# Choose best model
if rf_auc > lr_auc:
    print(f"\n🏆 Best Model: Random Forest (AUC: {rf_auc:.4f})")
    best_model = rf_model
    best_pred = rf_pred
    best_pred_proba = rf_pred_proba
    best_auc = rf_auc
    model_name = "RandomForest"
    use_scaler = False
else:
    print(f"\n🏆 Best Model: Logistic Regression (AUC: {lr_auc:.4f})")
    best_model = lr_model
    best_pred = lr_pred
    best_pred_proba = lr_pred_proba
    best_auc = lr_auc
    model_name = "LogisticRegression"
    use_scaler = True

# ============================================================================
# EVALUATE MODEL
# ============================================================================

print("\n📈 Model Evaluation...")

# Classification report
print("\nClassification Report:")
print(classification_report(y_test, best_pred, target_names=['Active', 'Churned']))

# Confusion matrix
cm = confusion_matrix(y_test, best_pred)
print("\nConfusion Matrix:")
print(f"              Predicted Active  Predicted Churned")
print(f"Actually Active:     {cm[0,0]:5d}           {cm[0,1]:5d}")
print(f"Actually Churned:    {cm[1,0]:5d}           {cm[1,1]:5d}")

# Feature importance (for Random Forest)
if model_name == "RandomForest":
    feature_importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': rf_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print("\n🔝 Top 10 Most Important Features:")
    for idx, row in feature_importance.head(10).iterrows():
        print(f"  {row['feature']:25s}: {row['importance']:.4f}")

# ============================================================================
# SAVE MODEL
# ============================================================================

print("\n💾 Saving model...")

models_dir = os.path.join(base_dir, 'models')
os.makedirs(models_dir, exist_ok=True)

model_path = os.path.join(models_dir, 'churn_prediction_model.pkl')
with open(model_path, 'wb') as f:
    pickle.dump({
        'model': best_model,
        'scaler': scaler if use_scaler else None,
        'feature_cols': feature_cols,
        'model_name': model_name,
        'metrics': {
            'auc': best_auc,
            'confusion_matrix': cm.tolist()
        }
    }, f)

print(f"✓ Model saved to: {model_path}")

# Generate predictions for all users
print("\n📝 Generating churn predictions for all users...")
X_all = df_features[feature_cols]
if use_scaler:
    X_all_scaled = scaler.transform(X_all)
    churn_probabilities = best_model.predict_proba(X_all_scaled)[:, 1]
else:
    churn_probabilities = best_model.predict_proba(X_all)[:, 1]

df_features['churn_probability'] = churn_probabilities
df_features['churn_risk'] = pd.cut(churn_probabilities, bins=[0, 0.3, 0.7, 1.0], labels=['Low', 'Medium', 'High'])

# Save predictions
predictions_path = os.path.join(models_dir, 'churn_predictions.csv')
df_features[['user_id', 'churn_probability', 'churn_risk', 'recency_days', 'frequency', 'total_spent']].to_csv(predictions_path, index=False)
print(f"✓ Predictions saved to: {predictions_path}")

# Summary by risk level
print("\n📊 Churn Risk Distribution:")
risk_summary = df_features['churn_risk'].value_counts()
for risk, count in risk_summary.items():
    print(f"  {risk:8s} Risk: {count:6,} users ({count/len(df_features)*100:5.1f}%)")

# ============================================================================
# CREATE VISUALIZATIONS
# ============================================================================

print("\n📉 Creating visualizations...")

# ROC Curve
plt.figure(figsize=(10, 6))
fpr, tpr, _ = roc_curve(y_test, best_pred_proba)
plt.plot(fpr, tpr, label=f'{model_name} (AUC = {best_auc:.3f})', linewidth=2)
plt.plot([0, 1], [0, 1], 'k--', label='Random Classifier')
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')
plt.title('ROC Curve - Churn Prediction Model')
plt.legend()
plt.grid(True, alpha=0.3)
roc_path = os.path.join(models_dir, 'churn_roc_curve.png')
plt.savefig(roc_path, dpi=300, bbox_inches='tight')
print(f"✓ ROC curve saved to: {roc_path}")

# Feature importance plot (if Random Forest)
if model_name == "RandomForest":
    plt.figure(figsize=(12, 8))
    top_features = feature_importance.head(15)
    plt.barh(range(len(top_features)), top_features['importance'])
    plt.yticks(range(len(top_features)), top_features['feature'])
    plt.xlabel('Feature Importance')
    plt.title('Top 15 Most Important Features for Churn Prediction')
    plt.tight_layout()
    fi_path = os.path.join(models_dir, 'churn_feature_importance.png')
    plt.savefig(fi_path, dpi=300, bbox_inches='tight')
    print(f"✓ Feature importance plot saved to: {fi_path}")

# ============================================================================
# SUMMARY
# ============================================================================

print("\n" + "="*80)
print("✅ CHURN PREDICTION MODEL COMPLETE!")
print("="*80)
print()
print("📊 MODEL SUMMARY:")
print(f"  • Algorithm: {model_name}")
print(f"  • Features: {len(feature_cols)}")
print(f"  • Training Users: {len(X_train):,}")
print(f"  • Test AUC: {best_auc:.4f}")
print(f"  • Churn Definition: No purchase in {churn_threshold_days} days")
print()
print("📁 OUTPUT FILES:")
print(f"  • Model: {model_path}")
print(f"  • Predictions: {predictions_path}")
print(f"  • ROC Curve: {roc_path}")
if model_name == "RandomForest":
    print(f"  • Feature Importance: {fi_path}")
print()
print("🎯 BUSINESS ACTIONS:")
print("  • HIGH Risk Users: Send retention offers, discounts")
print("  • MEDIUM Risk Users: Re-engagement campaigns, personalized emails")
print("  • LOW Risk Users: Upsell opportunities, loyalty program")
print()
print("🔄 NEXT STEPS:")
print("  1. Deploy model to production API")
print("  2. Schedule daily prediction updates")
print("  3. A/B test retention campaigns")
print("  4. Retrain model monthly with new data")
print()
print("="*80)
