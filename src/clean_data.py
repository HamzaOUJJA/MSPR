import os
import calendar
import pandas as pd
from pyspark.sql import SparkSession



def clean_data(year, month):
    spark = SparkSession.builder \
        .appName("amazing_MSPR1") \
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    raw_data_dir = os.path.join("..", "data", "raw")
    file_path = os.path.join(raw_data_dir, f"{year}-{calendar.month_abbr[month]}.csv.gz")

    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    df['event_time'] = pd.to_datetime(df['event_time'], utc=True)

    df['brand'] = df['brand'].fillna('unknown')
    df['category_code'] = df['category_code'].fillna('unknown')

    df.drop_duplicates(inplace=True)
    df.dropna(subset=['user_id', 'product_id', 'user_session'], inplace=True)

    df = df[df['price'] >= 0]







