def dump_to_minio_spark(year, month):
    import os
    import calendar
    from pyspark.sql import SparkSession

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

        object_name = os.path.relpath(file_path, os.path.abspath("../data/raw"))
        client.fput_object(bucket, object_name, file_path)
        print(f"\033[1;32m[SPARK WORKER] Uploaded {object_name}\033[0m")

    # Build file name based on year and month
    file_name = f"{year}-{calendar.month_abbr[month]}.csv.gz"
    file_path = os.path.abspath(os.path.join("..", "data", "raw", file_name))

    if not os.path.exists(file_path):
        print(f"\033[1;31m[ERROR] File {file_name} not found in ../data/raw\033[0m")
        return

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("SparkMinIOUploader") \
        .master("local[*]") \
        .getOrCreate()

    sc = spark.sparkContext

    # Upload the single file
    sc.parallelize([file_path]).foreach(upload_to_minio)

    spark.stop()
