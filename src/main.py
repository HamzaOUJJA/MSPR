import time
from grab_data import grab_data
from dump_to_minio import dump_to_minio
from dump_to_minio_spark import dump_to_minio_spark



try:
    # Optionally download files
    # grab_data(2019, 10)
    # grab_data(2019, 11)
    # grab_data(2019, 12)

    time_1 = time.time()
    dump_to_minio_spark()  # or dump_to_minio_spark()
    time_2 = time.time() - time_1

    print(f"\033[1;34m⏱️ Time taken for upload: {time_2:.2f} seconds\033[0m")
except Exception as e:
    print(f"\033[1;31m❌ An error occurred: {e}\033[0m")
