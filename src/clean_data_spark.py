import os
import calendar
import shutil # Import shutil for directory removal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, lower, year, month






def clean_data_spark(y, m):
    spark = SparkSession.builder \
        .appName("amazing_MSPR1") \
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    raw_data_dir = os.path.join("..", "data", "raw")
    file_path = os.path.join(raw_data_dir, f"{y}-{calendar.month_abbr[m]}.csv.gz")
    print("Step 1: Reading CSV file...")

    df = spark.read.csv(file_path, header=True, inferSchema=True)
    print("Step 2: Calculating price quantiles...")

    quantiles = df.approxQuantile("price", [0.25, 0.75], 0.01)
    q1, q3 = quantiles
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    print("Step 3: Cleaning and transforming data...")

    df_clean = (
        df.fillna({'brand': 'unknown', 'category_code': 'unknown'})
          .dropna(subset=['user_id', 'product_id', 'user_session'])
          .dropDuplicates()
          .filter((col("price") >= 0) & (col("price").isNotNull()))
          .filter((col("price") >= lower_bound) & (col("price") <= upper_bound))
          .withColumn('event_time', to_timestamp('event_time'))
          .withColumn('brand', trim(lower(col('brand'))))
          .withColumn('category_code', trim(lower(col('category_code'))))
          .withColumn('event_year', year('event_time'))
          .withColumn('event_month', month('event_time'))
    )

    print("Step 4: Saving cleaned data to a temporary directory...")

    # Define the base output directory for cleaned data
    base_output_dir = os.path.join("..", "data", "cleaned")
    # Define a temporary directory for Spark's output
    temp_output_dir = os.path.join(base_output_dir, f"temp_cleaned_{y}-{calendar.month_abbr[m]}_csv")
    
    # Define the final desired single file name
    final_output_file_name = f"cleaned_{y}-{calendar.month_abbr[m]}.csv.gz"
    final_output_file_path = os.path.join(base_output_dir, final_output_file_name)

    # Ensure the base output directory exists
    os.makedirs(base_output_dir, exist_ok=True)

    # Coalesce to 1 partition and write to the temporary directory
    # This will create 'temp_cleaned_YYYY-Mon_csv/part-00000-....csv.gz'
    df_clean.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .option("compression", "gzip") \
        .csv(temp_output_dir) 
    
    print(f"Step 5: Moving the single part file to its final destination: {final_output_file_path}")

    # After Spark writes, the actual data file will be inside temp_output_dir
    # We need to find that single part file
    part_files = [f for f in os.listdir(temp_output_dir) if f.startswith('part-') and f.endswith('.csv.gz')]

    if len(part_files) == 1:
        source_file_path = os.path.join(temp_output_dir, part_files[0])
        # Move and rename the file
        os.rename(source_file_path, final_output_file_path)
        # Remove the temporary directory
        shutil.rmtree(temp_output_dir)
        print(f"✅ Cleaned data saved as single compressed CSV file: {final_output_file_path}")
    else:
        print(f"⚠️ Warning: Expected 1 part file but found {len(part_files)}. Data remains in temporary directory: {temp_output_dir}")

    spark.stop()