from pyspark.sql import SparkSession
from minio import Minio
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" pyspark-shell'
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
def upload_to_minio(file_path):
    from minio import Minio
    import os

    client = Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )
    bucket = "mspr-minio-bucket"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    object_name = os.path.relpath(file_path, os.path.abspath("../data"))
    client.fput_object(bucket, object_name, file_path)
    print(f"[SPARK WORKER] Uploaded {object_name}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SparkMinIOUploader") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext

    # Lister les fichiers localement
    data_dir = os.path.abspath("../data")
    file_paths = []
    for root, _, files in os.walk(data_dir):
        for file in files:
            file_paths.append(os.path.join(root, file))

    # Distribuer via Spark
    rdd = sc.parallelize(file_paths)
    rdd.foreach(upload_to_minio)

    spark.stop()
