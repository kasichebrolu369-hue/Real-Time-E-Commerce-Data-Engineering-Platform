"""
Spark ML - Customer Churn Prediction
====================================

PURPOSE:
Build a machine learning model to predict customer churn
using logistic regression and other classification algorithms.

FEATURES:
- Feature engineering from user behavior
- Train classification models
- Evaluate model performance
- Generate churn predictions for active users

USAGE:
spark-submit spark_ml_churn_prediction.py --train --predict
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
import argparse
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """ML configuration"""
    
    # Data Paths
    USERS_PATH = "/ecommerce/warehouse/dim_users/"
    TRANSACTIONS_PATH = "/ecommerce/warehouse/fact_transactions/"
    CLICKSTREAM_PATH = "/ecommerce/warehouse/fact_clickstream/"
    
    # Model Paths
    MODEL_PATH = "/ecommerce/models/churn/lr_model"
    PREDICTIONS_PATH = "/ecommerce/analytics/ml/churn_predictions/"
    
    # Feature Engineering
    CHURN_THRESHOLD_DAYS = 90  # No activity in 90 days = churned


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """Create Spark session for ML"""
    spark = SparkSession.builder \
        .appName("ChurnPrediction-LogisticRegression") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✓ Spark ML Session Created")
    return spark


# ============================================================================
# FEATURE ENGINEERING
# ============================================================================

def engineer_features(spark):
    """
    Create features for churn prediction
    
    Returns:
        DataFrame with features and target
    """
    logger.info("\n" + "="*80)
    logger.info("FEATURE ENGINEERING")
    logger.info("="*80)
    
    # Load data
    logger.info("Loading data...")
    users_df = spark.read.parquet(Config.USERS_PATH).filter(col("is_current") == True)
    transactions_df = spark.read.parquet(Config.TRANSACTIONS_PATH)
    clickstream_df = spark.read.parquet(Config.CLICKSTREAM_PATH)
    
    # Calculate user activity metrics
    logger.info("Calculating user metrics...")
    
    # Transaction metrics
    transaction_metrics = transactions_df.filter(col("status") == "completed") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_transactions"),
            sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            max("transaction_ts").alias("last_transaction_date"),
            datediff(current_date(), max("transaction_ts")).alias("days_since_last_purchase"),
            countDistinct("product_id").alias("unique_products_purchased"),
            avg("discount_amount").alias("avg_discount_used")
        )
    
    # Clickstream metrics
    clickstream_metrics = clickstream_df \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
            sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("total_cart_adds"),
            max("event_timestamp").alias("last_event_date"),
            datediff(current_date(), max(col("event_timestamp").cast("date"))).alias("days_since_last_activity")
        )
    
    # Join all metrics
    features_df = users_df.select(
        "user_id",
        "age",
        "customer_segment",
        "total_purchases",
        "lifetime_value",
        "account_created_at"
    ) \
    .join(transaction_metrics, on="user_id", how="left") \
    .join(clickstream_metrics, on="user_id", how="left")
    
    # Fill nulls
    features_df = features_df.fillna({
        "total_transactions": 0,
        "total_spent": 0.0,
        "avg_order_value": 0.0,
        "days_since_last_purchase": 999,
        "unique_products_purchased": 0,
        "avg_discount_used": 0.0,
        "total_events": 0,
        "total_sessions": 0,
        "total_views": 0,
        "total_cart_adds": 0,
        "days_since_last_activity": 999
    })
    
    # Create target variable (churned)
    features_df = features_df.withColumn(
        "churned",
        when(col("days_since_last_activity") > Config.CHURN_THRESHOLD_DAYS, 1).otherwise(0)
    )
    
    # Derived features
    features_df = features_df \
        .withColumn("account_age_days", datediff(current_date(), col("account_created_at"))) \
        .withColumn("purchase_frequency", col("total_transactions") / col("account_age_days")) \
        .withColumn("avg_session_events", col("total_events") / col("total_sessions")) \
        .withColumn("cart_to_purchase_ratio", 
                   col("total_transactions") / (col("total_cart_adds") + 1)) \
        .withColumn("engagement_score", 
                   (col("total_events") + col("total_transactions") * 10) / col("account_age_days"))
    
    # Encode categorical features
    segment_indexer = StringIndexer(inputCol="customer_segment", outputCol="segment_index")
    features_df = segment_indexer.fit(features_df).transform(features_df)
    
    logger.info(f"✓ Features engineered for {features_df.count():,} users")
    
    # Show churn distribution
    churn_dist = features_df.groupBy("churned").count()
    logger.info("\nChurn Distribution:")
    churn_dist.show()
    
    return features_df


# ============================================================================
# MODEL TRAINING
# ============================================================================

def train_churn_model(features_df):
    """
    Train churn prediction model
    
    Args:
        features_df: DataFrame with features
    
    Returns:
        Trained pipeline model
    """
    logger.info("\n" + "="*80)
    logger.info("TRAINING CHURN MODEL")
    logger.info("="*80)
    
    # Select features
    feature_cols = [
        "age",
        "segment_index",
        "total_transactions",
        "total_spent",
        "avg_order_value",
        "days_since_last_purchase",
        "unique_products_purchased",
        "avg_discount_used",
        "total_events",
        "total_sessions",
        "total_views",
        "total_cart_adds",
        "days_since_last_activity",
        "account_age_days",
        "purchase_frequency",
        "avg_session_events",
        "cart_to_purchase_ratio",
        "engagement_score"
    ]
    
    logger.info(f"Features: {len(feature_cols)}")
    logger.info(f"  {', '.join(feature_cols[:5])}...")
    
    # Split data
    train_df, test_df = features_df.randomSplit([0.8, 0.2], seed=42)
    
    logger.info(f"\nTraining set: {train_df.count():,} users")
    logger.info(f"Test set: {test_df.count():,} users")
    
    # Build pipeline
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    
    # Logistic Regression
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="churned",
        maxIter=100,
        regParam=0.01,
        elasticNetParam=0.5
    )
    
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    # Train
    logger.info("\nTraining Logistic Regression model...")
    model = pipeline.fit(train_df)
    
    logger.info("✓ Model training complete")
    
    return model, train_df, test_df, feature_cols


def evaluate_model(model, test_df):
    """
    Evaluate model performance
    
    Args:
        model: Trained model
        test_df: Test DataFrame
    """
    logger.info("\n" + "="*80)
    logger.info("EVALUATING MODEL")
    logger.info("="*80)
    
    # Make predictions
    predictions = model.transform(test_df)
    
    # Binary classification metrics
    evaluator_auc = BinaryClassificationEvaluator(labelCol="churned", metricName="areaUnderROC")
    auc = evaluator_auc.evaluate(predictions)
    
    logger.info(f"  Area Under ROC: {auc:.4f}")
    
    # Multiclass metrics
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="churned", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator_acc.evaluate(predictions)
    
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="churned", predictionCol="prediction", metricName="weightedPrecision")
    precision = evaluator_prec.evaluate(predictions)
    
    evaluator_rec = MulticlassClassificationEvaluator(labelCol="churned", predictionCol="prediction", metricName="weightedRecall")
    recall = evaluator_rec.evaluate(predictions)
    
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="churned", predictionCol="prediction", metricName="f1")
    f1 = evaluator_f1.evaluate(predictions)
    
    logger.info(f"  Accuracy: {accuracy:.4f}")
    logger.info(f"  Precision: {precision:.4f}")
    logger.info(f"  Recall: {recall:.4f}")
    logger.info(f"  F1 Score: {f1:.4f}")
    
    # Confusion matrix
    logger.info("\n  Confusion Matrix:")
    predictions.groupBy("churned", "prediction").count().show()
    
    # Sample predictions
    logger.info("\n  Sample Predictions:")
    predictions.select(
        "user_id", "churned", "prediction", "probability"
    ).show(20, truncate=False)
    
    # Feature importance (from LR coefficients)
    lr_model = model.stages[-1]
    coefficients = lr_model.coefficients
    
    logger.info("\n  Top 10 Most Important Features:")
    # Note: Would need feature names mapping for better display
    
    return predictions


# ============================================================================
# GENERATE PREDICTIONS
# ============================================================================

def predict_churn(spark, model, feature_cols):
    """
    Generate churn predictions for all active users
    
    Args:
        spark: SparkSession
        model: Trained model
        feature_cols: List of feature column names
    
    Returns:
        DataFrame with predictions
    """
    logger.info("\n" + "="*80)
    logger.info("GENERATING CHURN PREDICTIONS")
    logger.info("="*80)
    
    # Get current features for all users
    features_df = engineer_features(spark)
    
    # Filter only non-churned users
    active_users = features_df.filter(col("churned") == 0)
    
    logger.info(f"Predicting churn for {active_users.count():,} active users...")
    
    # Make predictions
    predictions = model.transform(active_users)
    
    # Extract churn probability
    def extract_prob(probability):
        return float(probability[1])
    
    extract_prob_udf = udf(extract_prob, FloatType())
    
    predictions = predictions.withColumn(
        "churn_probability",
        extract_prob_udf(col("probability"))
    )
    
    # Select output columns
    output_df = predictions.select(
        "user_id",
        "customer_segment",
        "lifetime_value",
        "days_since_last_activity",
        "total_transactions",
        "engagement_score",
        "prediction",
        "churn_probability"
    ).withColumn(
        "risk_level",
        when(col("churn_probability") > 0.8, "High")
        .when(col("churn_probability") > 0.5, "Medium")
        .otherwise("Low")
    )
    
    # Show high-risk users
    logger.info("\n  High Risk Users (Top 20):")
    output_df.filter(col("risk_level") == "High") \
        .orderBy(col("churn_probability").desc()) \
        .show(20, truncate=False)
    
    return output_df


def save_predictions(predictions_df):
    """
    Save predictions to HDFS
    
    Args:
        predictions_df: Predictions DataFrame
    """
    logger.info("\n" + "="*80)
    logger.info("SAVING PREDICTIONS")
    logger.info("="*80)
    
    predictions_df.write \
        .mode("overwrite") \
        .partitionBy("risk_level") \
        .parquet(Config.PREDICTIONS_PATH)
    
    logger.info(f"✓ Predictions saved to: {Config.PREDICTIONS_PATH}")


def save_model(model):
    """
    Save trained model
    
    Args:
        model: Trained pipeline model
    """
    logger.info("\n" + "="*80)
    logger.info("SAVING MODEL")
    logger.info("="*80)
    
    model.write().overwrite().save(Config.MODEL_PATH)
    
    logger.info(f"✓ Model saved to: {Config.MODEL_PATH}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Churn Prediction Model')
    parser.add_argument('--train', action='store_true', help='Train new model')
    parser.add_argument('--evaluate', action='store_true', help='Evaluate model')
    parser.add_argument('--predict', action='store_true', help='Generate predictions')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("SPARK ML - CUSTOMER CHURN PREDICTION")
    logger.info("="*80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        if args.train or args.evaluate:
            # Engineer features
            features_df = engineer_features(spark)
            
            # Train model
            model, train_df, test_df, feature_cols = train_churn_model(features_df)
            
            # Evaluate
            if args.evaluate:
                evaluate_model(model, test_df)
            
            # Save model
            save_model(model)
        
        if args.predict:
            # Load model
            from pyspark.ml import PipelineModel
            model = PipelineModel.load(Config.MODEL_PATH)
            
            # Feature columns (would be saved with model in production)
            feature_cols = [
                "age", "segment_index", "total_transactions", "total_spent",
                "avg_order_value", "days_since_last_purchase", 
                "unique_products_purchased", "avg_discount_used",
                "total_events", "total_sessions", "total_views",
                "total_cart_adds", "days_since_last_activity",
                "account_age_days", "purchase_frequency",
                "avg_session_events", "cart_to_purchase_ratio",
                "engagement_score"
            ]
            
            # Generate predictions
            predictions = predict_churn(spark, model, feature_cols)
            save_predictions(predictions)
        
        logger.info("\n" + "="*80)
        logger.info("✅ CHURN PREDICTION COMPLETE")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
