import os
import calendar
import shutil # Import shutil for directory removal
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, lower, year, month





def clean_data(y, m):
    # ⏬ Parameters moved inside the function
    chunk_fraction = 0.001  # = 0.1%
    max_chunks = 100        # max number of chunks to process

    spark = SparkSession.builder \
        .appName("amazing_MSPR1") \
        .config("spark.driver.memory", "20g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
        .getOrCreate()

    raw_data_dir = os.path.join("..", "data", "raw")
    file_path = os.path.join(raw_data_dir, f"{y}-{calendar.month_abbr[m]}.csv.gz")
    print(f"Reading file: {file_path}")

    # Read once for schema and quantiles
    df_full = spark.read.csv(file_path, header=True, inferSchema=True)

    total_rows = df_full.count()
    print(f"📊 Total rows in full dataset: {total_rows}")

    print("Calculating price quantiles...")
    quantiles = df_full.approxQuantile("price", [0.25, 0.75], 0.01)
    q1, q3 = quantiles
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    del df_full  # Free memory

    base_output_dir = os.path.join("..", "data", "cleaned")
    final_output_dir = os.path.join(base_output_dir, f"chunk_cleaned_{y}-{calendar.month_abbr[m]}_dir")
    os.makedirs(final_output_dir, exist_ok=True)

    print("🚀 Starting to clean chunks...")
    processed_rows = 0

    for chunk_id in range(max_chunks):
        print(f"\n▶️ Chunk {chunk_id + 1}/{max_chunks}")
        df_chunk = spark.read.csv(file_path, header=True, inferSchema=True).sample(fraction=chunk_fraction, seed=chunk_id)

        chunk_count = df_chunk.count()
        if chunk_count == 0:
            print("No more data to process or chunk was empty.")
            break

        df_clean = (
            df_chunk.fillna({'brand': 'unknown', 'category_code': 'unknown'})
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

        if df_clean.rdd.isEmpty():
            print("Chunk cleaned to empty — skipping write.")
            continue

        chunk_output_path = os.path.join(final_output_dir, f"chunk_{chunk_id}.csv.gz")
        df_clean.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("compression", "gzip") \
            .csv(chunk_output_path)

        part_file = [f for f in os.listdir(chunk_output_path) if f.startswith('part-') and f.endswith('.csv.gz')]
        if part_file:
            os.rename(
                os.path.join(chunk_output_path, part_file[0]),
                os.path.join(final_output_dir, f"cleaned_chunk_{chunk_id}.csv.gz")
            )
            shutil.rmtree(chunk_output_path)
            print(f"✅ Saved: cleaned_chunk_{chunk_id}.csv.gz")
        else:
            print(f"⚠️ Chunk {chunk_id} was written but no part file found.")

        # Update and print progress
        processed_rows += chunk_count
        percent_processed = min(100.0, (processed_rows / total_rows) * 100)
        print(f"📈 Progress: {processed_rows:,} rows processed (~{percent_processed:.2f}%)")

    print(f"\n🏁 All chunks processed and saved in: {final_output_dir}")
    spark.stop()




























def clean_data_1(y, m):
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