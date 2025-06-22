import time
import os
from pyspark.sql import SparkSession
from grab_data import grab_data
from dump_to_minio import dump_to_minio

if __name__ == "__main__":
    # Set critical environment variables
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" pyspark-shell'
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
    
    spark = SparkSession.builder \
    .appName("GrabAndDumpJob") \
    .master("spark://spark-master:7077") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.driver.host", "spark-driver") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "2g") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .getOrCreate()

    try:
        start_time = time.time()
        grab_data(2019, 10)
        time_1 = time.time() - start_time
        
        dump_to_minio()
        time_2 = time.time() - start_time

        print(f"Data processing time: {time_1:.2f} seconds")
        print(f"Total execution time: {time_2:.2f} seconds")
    finally:
        spark.stop()