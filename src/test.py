import time
from grab_data import grab_data
from dump_to_minio import dump_to_minio


start_time = time.time()
grab_data(2019, 10)
time_1 = time.time() - start_time

# dump_to_minio()
# time_2 = time.time() - start_time

print(f"Data processing time: {time_1:.2f} seconds")

