import os
from pyspark.sql import SparkSession





def dump_to_minio_spark():
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
            print(f"\033[1;34m[SPARK WORKER] Bucket '{bucket}' created.\033[0m")

        object_name = os.path.relpath(file_path, os.path.abspath("../data"))
        client.fput_object(bucket, object_name, file_path)
        print(f"\033[1;32m[SPARK WORKER] Uploaded {object_name}\033[0m")

    spark = SparkSession.builder \
        .appName("SparkMinIOUploader") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext

    data_dir = os.path.abspath("../data")
    file_paths = [
        os.path.join(root, file)
        for root, _, files in os.walk(data_dir)
        for file in files
    ]

    print(f"\033[1;36m[MAIN] Found {len(file_paths)} file(s) to upload.\033[0m")

    sc.parallelize(file_paths).foreach(upload_to_minio)

    print("\033[1;35m[MAIN] Upload job finished. Stopping Spark.\033[0m")
    spark.stop()
