"""
Spark ML - Recommendation Engine (ALS)
======================================

PURPOSE:
Build a collaborative filtering recommendation system using
Alternating Least Squares (ALS) algorithm.

FEATURES:
- Train on transaction/clickstream data
- Generate product recommendations for users
- Evaluate model performance
- Save model for production deployment

USAGE:
spark-submit spark_ml_recommendations.py --train --evaluate
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
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
    TRANSACTIONS_PATH = "/ecommerce/warehouse/fact_transactions/"
    CLICKSTREAM_PATH = "/ecommerce/warehouse/fact_clickstream/"
    USERS_PATH = "/ecommerce/warehouse/dim_users/"
    PRODUCTS_PATH = "/ecommerce/warehouse/dim_products/"
    
    # Model Paths
    MODEL_PATH = "/ecommerce/models/recommendations/als_model"
    RECOMMENDATIONS_PATH = "/ecommerce/analytics/ml/recommendations/"
    
    # ALS Parameters
    RANK = 10
    MAX_ITER = 10
    REG_PARAM = 0.1
    ALPHA = 1.0
    
    # Recommendation Parameters
    NUM_RECOMMENDATIONS = 10


# ============================================================================
# SPARK SESSION
# ============================================================================

def create_spark_session():
    """Create Spark session for ML"""
    spark = SparkSession.builder \
        .appName("RecommendationEngine-ALS") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✓ Spark ML Session Created")
    return spark


# ============================================================================
# DATA PREPARATION
# ============================================================================

def load_and_prepare_data(spark):
    """
    Load and prepare training data
    
    Returns:
        DataFrame with user_id, product_id, rating
    """
    logger.info("\n" + "="*80)
    logger.info("LOADING AND PREPARING DATA")
    logger.info("="*80)
    
    # Load transactions
    logger.info("Loading transactions...")
    transactions_df = spark.read.parquet(Config.TRANSACTIONS_PATH)
    
    # Load clickstream for implicit ratings
    logger.info("Loading clickstream...")
    clickstream_df = spark.read.parquet(Config.CLICKSTREAM_PATH)
    
    # Create explicit ratings from transactions
    # Rating = 5 for purchases, normalized by quantity
    transaction_ratings = transactions_df.filter(col("status") == "completed") \
        .select(
            col("user_id"),
            col("product_id"),
            (lit(5.0) * col("quantity")).alias("rating")
        )
    
    logger.info(f"  Transaction ratings: {transaction_ratings.count():,}")
    
    # Create implicit ratings from clickstream
    # view=1, add_to_cart=3, purchase=5
    clickstream_ratings = clickstream_df \
        .withColumn(
            "rating",
            when(col("event_type") == "purchase", 5.0)
            .when(col("event_type") == "add_to_cart", 3.0)
            .when(col("event_type") == "view", 1.0)
            .otherwise(0.0)
        ) \
        .filter(col("rating") > 0) \
        .filter(col("product_id").isNotNull()) \
        .select("user_id", "product_id", "rating")
    
    logger.info(f"  Clickstream ratings: {clickstream_ratings.count():,}")
    
    # Combine and aggregate
    all_ratings = transaction_ratings.union(clickstream_ratings)
    
    # Aggregate by user-product, sum ratings
    aggregated_ratings = all_ratings.groupBy("user_id", "product_id") \
        .agg(sum("rating").alias("rating"))
    
    # Normalize ratings to 1-5 scale
    max_rating = aggregated_ratings.agg(max("rating")).collect()[0][0]
    
    final_ratings = aggregated_ratings.withColumn(
        "rating",
        (col("rating") / max_rating) * 5.0
    )
    
    logger.info(f"  Final ratings: {final_ratings.count():,}")
    
    # Convert user_id and product_id to numeric indices
    from pyspark.ml.feature import StringIndexer
    
    user_indexer = StringIndexer(inputCol="user_id", outputCol="user_index")
    product_indexer = StringIndexer(inputCol="product_id", outputCol="product_index")
    
    final_ratings = user_indexer.fit(final_ratings).transform(final_ratings)
    final_ratings = product_indexer.fit(final_ratings).transform(final_ratings)
    
    # Select final columns
    final_ratings = final_ratings.select(
        col("user_index").cast("int").alias("user_id"),
        col("product_index").cast("int").alias("product_id"),
        col("rating").cast("float")
    )
    
    # Cache for training
    final_ratings.cache()
    
    logger.info("✓ Data preparation complete")
    logger.info(f"  Unique users: {final_ratings.select('user_id').distinct().count():,}")
    logger.info(f"  Unique products: {final_ratings.select('product_id').distinct().count():,}")
    
    return final_ratings


# ============================================================================
# MODEL TRAINING
# ============================================================================

def train_als_model(ratings_df):
    """
    Train ALS recommendation model
    
    Args:
        ratings_df: DataFrame with user_id, product_id, rating
    
    Returns:
        Trained ALS model
    """
    logger.info("\n" + "="*80)
    logger.info("TRAINING ALS MODEL")
    logger.info("="*80)
    
    # Split data
    (training, test) = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    logger.info(f"Training set: {training.count():,} ratings")
    logger.info(f"Test set: {test.count():,} ratings")
    
    # Build ALS model
    als = ALS(
        rank=Config.RANK,
        maxIter=Config.MAX_ITER,
        regParam=Config.REG_PARAM,
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        implicitPrefs=False,
        nonnegative=True
    )
    
    logger.info(f"\nALS Parameters:")
    logger.info(f"  Rank: {Config.RANK}")
    logger.info(f"  Max Iterations: {Config.MAX_ITER}")
    logger.info(f"  Regularization: {Config.REG_PARAM}")
    
    # Train model
    logger.info("\nTraining model...")
    model = als.fit(training)
    
    logger.info("✓ Model training complete")
    
    return model, test


def evaluate_model(model, test_df):
    """
    Evaluate model performance
    
    Args:
        model: Trained ALS model
        test_df: Test DataFrame
    """
    logger.info("\n" + "="*80)
    logger.info("EVALUATING MODEL")
    logger.info("="*80)
    
    # Make predictions
    predictions = model.transform(test_df)
    
    # Evaluate using RMSE
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    
    logger.info(f"  Root Mean Square Error (RMSE): {rmse:.4f}")
    
    # Evaluate using MAE
    evaluator_mae = RegressionEvaluator(
        metricName="mae",
        labelCol="rating",
        predictionCol="prediction"
    )
    mae = evaluator_mae.evaluate(predictions)
    
    logger.info(f"  Mean Absolute Error (MAE): {mae:.4f}")
    
    # Sample predictions
    logger.info("\n  Sample Predictions:")
    predictions.select("user_id", "product_id", "rating", "prediction") \
        .show(10, truncate=False)
    
    return rmse, mae


# ============================================================================
# GENERATE RECOMMENDATIONS
# ============================================================================

def generate_recommendations(spark, model):
    """
    Generate product recommendations for all users
    
    Args:
        spark: SparkSession
        model: Trained ALS model
    
    Returns:
        DataFrame with recommendations
    """
    logger.info("\n" + "="*80)
    logger.info("GENERATING RECOMMENDATIONS")
    logger.info("="*80)
    
    # Generate top N recommendations for each user
    user_recs = model.recommendForAllUsers(Config.NUM_RECOMMENDATIONS)
    
    # Explode recommendations
    recs_exploded = user_recs.select(
        col("user_id"),
        explode(col("recommendations")).alias("rec")
    ).select(
        col("user_id"),
        col("rec.product_id").alias("product_id"),
        col("rec.rating").alias("predicted_rating")
    )
    
    logger.info(f"  Generated recommendations for {user_recs.count():,} users")
    
    # Sample recommendations
    logger.info("\n  Sample Recommendations:")
    recs_exploded.show(20, truncate=False)
    
    return recs_exploded


def save_recommendations(recommendations_df):
    """
    Save recommendations to HDFS
    
    Args:
        recommendations_df: Recommendations DataFrame
    """
    logger.info("\n" + "="*80)
    logger.info("SAVING RECOMMENDATIONS")
    logger.info("="*80)
    
    output_path = Config.RECOMMENDATIONS_PATH
    
    recommendations_df.write \
        .mode("overwrite") \
        .partitionBy("user_id") \
        .parquet(output_path)
    
    logger.info(f"✓ Recommendations saved to: {output_path}")


def save_model(model):
    """
    Save trained model
    
    Args:
        model: Trained ALS model
    """
    logger.info("\n" + "="*80)
    logger.info("SAVING MODEL")
    logger.info("="*80)
    
    model.write().overwrite().save(Config.MODEL_PATH)
    
    logger.info(f"✓ Model saved to: {Config.MODEL_PATH}")


# ============================================================================
# HYPERPARAMETER TUNING (OPTIONAL)
# ============================================================================

def tune_hyperparameters(training_df, test_df):
    """
    Perform hyperparameter tuning using cross-validation
    
    Args:
        training_df: Training DataFrame
        test_df: Test DataFrame
    
    Returns:
        Best model
    """
    logger.info("\n" + "="*80)
    logger.info("HYPERPARAMETER TUNING")
    logger.info("="*80)
    
    als = ALS(
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        implicitPrefs=False,
        nonnegative=True
    )
    
    # Parameter grid
    param_grid = ParamGridBuilder() \
        .addGrid(als.rank, [5, 10, 15]) \
        .addGrid(als.regParam, [0.01, 0.1, 0.5]) \
        .addGrid(als.maxIter, [5, 10, 15]) \
        .build()
    
    # Evaluator
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    # Cross-validator
    cv = CrossValidator(
        estimator=als,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=4
    )
    
    logger.info("Running cross-validation...")
    cv_model = cv.fit(training_df)
    
    # Best model
    best_model = cv_model.bestModel
    
    logger.info(f"✓ Best parameters found:")
    logger.info(f"  Rank: {best_model.rank}")
    logger.info(f"  Regularization: {best_model._java_obj.parent().getRegParam()}")
    logger.info(f"  Max Iterations: {best_model._java_obj.parent().getMaxIter()}")
    
    return best_model


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='ALS Recommendation Engine')
    parser.add_argument('--train', action='store_true', help='Train new model')
    parser.add_argument('--evaluate', action='store_true', help='Evaluate model')
    parser.add_argument('--tune', action='store_true', help='Hyperparameter tuning')
    parser.add_argument('--recommend', action='store_true', help='Generate recommendations')
    
    args = parser.parse_args()
    
    logger.info("="*80)
    logger.info("SPARK ML - RECOMMENDATION ENGINE (ALS)")
    logger.info("="*80)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Load and prepare data
        ratings_df = load_and_prepare_data(spark)
        
        if args.train or args.evaluate:
            # Train model
            if args.tune:
                training, test = ratings_df.randomSplit([0.8, 0.2], seed=42)
                model = tune_hyperparameters(training, test)
            else:
                model, test = train_als_model(ratings_df)
            
            # Evaluate
            if args.evaluate:
                evaluate_model(model, test)
            
            # Save model
            save_model(model)
        
        # Load existing model
        from pyspark.ml.recommendation import ALSModel
        model = ALSModel.load(Config.MODEL_PATH)
        
        if args.recommend:
            # Generate recommendations
            recommendations = generate_recommendations(spark, model)
            save_recommendations(recommendations)
        
        logger.info("\n" + "="*80)
        logger.info("✅ RECOMMENDATION ENGINE COMPLETE")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
