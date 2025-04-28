from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import argparse
from config.config_loader import AWS_CONFIG, SPARK_CONFIG

def create_spark_session():
    return SparkSession.builder \
        .appName("User Segmentation ML") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_CONFIG['access_key_id']) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_CONFIG['secret']) \
        .config("spark.python.executable", SPARK_CONFIG['pyspark_python']) \
        .getOrCreate()

def prepare_features(df):
    
    feature_columns = [
        'total_events', 'total_searches', 'total_cart_adds',
        'total_purchases', 'avg_purchase_amount', 'total_sessions',
        'unique_pages_visited', 'desktop', 'mobile', 'tablet'
    ]
    
    
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )
    
    return assembler.transform(df)

def train_kmeans(df, num_clusters):
    
    kmeans = KMeans().setK(num_clusters).setFeaturesCol("features").setPredictionCol("segment")
    
    
    model = kmeans.fit(df)
    
    return model

def evaluate_model(model, df):
    predictions = model.transform(df)
    evaluator = ClusteringEvaluator(
        predictionCol="segment",
        featuresCol="features",
        metricName="silhouette"
    )
    
    silhouette = evaluator.evaluate(predictions)
    print(f"Silhouette score: {silhouette}")
    
    return predictions

def analyze_segments(predictions):
    
    segment_sizes = predictions.groupBy("segment").count()\
        .orderBy("segment")
    
    print("\nSegment Sizes:")
    segment_sizes.show()
    
    
    segment_stats = predictions.groupBy("segment").agg(
        *[avg(col).alias(col) for col in predictions.columns 
          if col not in ["segment", "features", "user_id"]]
    ).orderBy("segment")
    
    print("\nSegment Characteristics:")
    segment_stats.show()

def main():
    parser = argparse.ArgumentParser(description='User Segmentation ML')
    parser.add_argument('--features-path', required=True,help='Input path for user features')
    parser.add_argument('--output-path', required=True,help='Output path for user segments')
    parser.add_argument('--num-clusters', type=int, default=5,help='Number of clusters for K-means')
    
    args = parser.parse_args()
    spark = create_spark_session()
    feature_df = spark.read.parquet(args.features_path)
    prepared_df = prepare_features(feature_df)
    model = train_kmeans(prepared_df, args.num_clusters)
    predictions = evaluate_model(model, prepared_df)
    analyze_segments(predictions)
    predictions.select("user_id", "segment").write.mode("overwrite").parquet(args.output_path)
    
    spark.stop()

if __name__ == '__main__':
    main()