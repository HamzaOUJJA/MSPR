from pyspark.sql import SparkSession
import os
import time
# RDD
# spark_conf = SparkConf().setAppName('mspr')
# sc = SparkContext(conf=spark_conf)

# Dataframe

spark = SparkSession.builder.appName("mspr822").getOrCreate()

events = spark.read.option('header', True).csv(list(map(lambda x: f'/tmp/dataset/output/{x}', os.listdir('/tmp/dataset/output'))))

df_process_start = time.perf_counter()
events.groupBy('event_type').count().show()
df_process_end = time.perf_counter()

events.createOrReplaceTempView('events')

sql_process_start = time.perf_counter()
spark.sql('select event_type, count(event_type) from events group by event_type').show()
sql_process_end = time.perf_counter()


print(f'DataFrame processing time: {df_process_end - df_process_start} seconds')
print(f'SQL processing time: {sql_process_end - sql_process_start} seconds')