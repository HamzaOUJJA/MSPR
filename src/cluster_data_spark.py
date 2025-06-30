from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, count, countDistinct
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml import Pipeline
from pyspark.ml import PipelineModel
import calendar
import os
import shutil
import re


def get_latest_model_version(model_base_path: str) -> int:
    if not os.path.exists(model_base_path):
        return 0
    version_dirs = [d for d in os.listdir(model_base_path) if re.match(r'v\d+', d)]
    if not version_dirs:
        return 0
    return max(int(re.search(r'\d+', d).group()) for d in version_dirs)

def cluster_data_spark(year: int, month: int, n_clusters: int = 4):
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" pyspark-shell'
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    month_abbr = calendar.month_abbr[month]
    filename = f"cleaned_{year}-{month_abbr}.csv.gz"
    input_path = os.path.join("..", "data", "cleaned", filename)
    output_dir = os.path.join("..", "data", "clustered")
    os.makedirs(output_dir, exist_ok=True)
    temp_output_dir = os.path.join(output_dir, f"_temp_clustered_{year}-{month_abbr}")
    final_output_file_path = os.path.join(output_dir, f"clustered_{year}-{month_abbr}.csv.gz")

    if not os.path.exists(input_path):
        print(f"\033[1;31m❌ File does not exist: {input_path}\033[0m")
        return

    spark = SparkSession.builder.appName("ClusteringUsers").getOrCreate()

    print(f"\033[1;34m📂 Loading file: {input_path}\033[0m")
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    print("🔧 Aggregating features...")
    df_features = df.groupBy("user_id").agg(
        count(when(col("event_type") == "view", True)).alias("views"),
        count(when(col("event_type") == "cart", True)).alias("carts"),
        count(when(col("event_type") == "remove_from_cart", True)).alias("removals"),
        count(when(col("event_type") == "purchase", True)).alias("purchases"),
        countDistinct("user_session").alias("sessions"),
        _sum("price").alias("total_spent")
    ).na.fill(0)

    feature_cols = ["views", "carts", "removals", "purchases", "sessions", "total_spent"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_vec")
    scaler = StandardScaler(inputCol="features_vec", outputCol="scaled_features", withStd=True, withMean=True)

    # Model directory setup
    model_base_path = os.path.join("..", "Models", f"kmeans_model_k{n_clusters}")
    os.makedirs(model_base_path, exist_ok=True)

    latest_version = get_latest_model_version(model_base_path)
    model_path = os.path.join(model_base_path, f"v{latest_version}")

    print("🤖 Training new KMeans model...")
    kmeans = KMeans(featuresCol="scaled_features", predictionCol="cluster", k=n_clusters, seed=42)
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(df_features)

    next_version = latest_version + 1
    save_path = os.path.join(model_base_path, f"v{next_version}")
    print(f"💾 Saving model to {save_path}")
    model.save(save_path)

    clustered_features = model.transform(df_features).select("user_id", "cluster")

    print("🔗 Joining clusters to full data...")
    df_with_cluster = df.join(clustered_features, on="user_id", how="left")

    print("💾 Writing clustered data to compressed CSV...")
    df_with_cluster.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .csv(temp_output_dir)

    print(f"📦 Step 5: Moving the part file to {final_output_file_path}...")
    part_files = [f for f in os.listdir(temp_output_dir) if f.startswith("part-") and f.endswith(".csv.gz")]

    if len(part_files) == 1:
        shutil.move(os.path.join(temp_output_dir, part_files[0]), final_output_file_path)
        shutil.rmtree(temp_output_dir)
        print(f"\033[1;32m✅ Final clustered file saved: {final_output_file_path}\033[0m")
    else:
        print(f"\033[1;33m⚠️ Warning: Expected 1 part file but found {len(part_files)}. Check {temp_output_dir}\033[0m")

    spark.stop()
