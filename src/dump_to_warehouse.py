from pyspark.sql import SparkSession
import os
import calendar

def dump_to_warehouse(year: int, month: int):
    """
    Uploads a .csv.gz file for the specified year and month to PostgreSQL using Spark.
    Automatically sets the table name from the file name (without the 'cleaned_' prefix).
    """

    os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow" pyspark-shell'
    os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

    # Abbreviated month name like 'Oct'
    month_abbr = calendar.month_abbr[month]
    cleaned_file_name = f"clustered_{year}-{month_abbr}.csv.gz"
    file_path = os.path.abspath(os.path.join("..", "data", "clustered", cleaned_file_name))

    if not os.path.exists(file_path):
        print(f"\033[1;31mFile {file_path} does not exist.\033[0m")
        return

    # Table name derived from filename (without 'cleaned_' and extension, e.g. '2019-Oct')
    base_name = cleaned_file_name.replace("clustered_", "").replace(".csv.gz", "").replace("-", "_").lower()
    table_name = f"data_{base_name}"

    # Start Spark session with PostgreSQL driver
    spark = SparkSession.builder \
        .appName("UploadToPostgres") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/javax.security.auth=ALL-UNNAMED") \
        .getOrCreate()

    # Load CSV
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(file_path)

    print("\033[1;34mData preview:\033[0m")
    df.show(5)
    df.printSchema()

    # Upload to PostgreSQL
    print(f"\033[1;36mUploading to PostgreSQL table '{table_name}'...\033[0m")
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:15432/amazing_mspr_db") \
        .option("dbtable", table_name) \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    print("\033[1;32mUpload complete!\033[0m")
    spark.stop()
