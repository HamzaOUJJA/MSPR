import time
from pyspark.sql import SparkSession
from grab_data import grab_data
from dump_to_minio import dump_to_minio

if __name__ == "__main__":
    spark = SparkSession.builder.appName("GrabAndDumpJob").getOrCreate()

    start_time = time.time()  # start timer

    # Your code here - currently just calls functions
    grab_data(2019, 10)
    time_1 = time.time() - start_time
    dump_to_minio()    

    time_2 = time.time() - start_time  # end timer
    

    print(f"Total execution time: {time_1:.2f} seconds")
    print(f"Total execution time: {time_2:.2f} seconds")

    spark.stop()
