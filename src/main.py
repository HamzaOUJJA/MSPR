import time
from grab_data import grab_data
from dump_to_minio import dump_to_minio
from dump_to_minio_spark import dump_to_minio_spark
from load_data_from_minio import load_data_from_minio
from load_data import load_data


try:

    time_1 = time.time()

    df_oct = load_data(2019, 10)
    #df_nov = load_data(2019, 11)
    #df_dec = load_data(2019, 12)
    #df_jan = load_data(2020, 1) 
    #df_feb = load_data(2020, 2) 
    #df_mar = load_data(2020, 3) 
    #df_apr = load_data(2020, 4) 

    df = (df_oct
            #.union(df_nov)
            #.union(df_dec)
            #.union(df_jan)
            #.union(df_feb)
            #.union(df_mar)
            #.union(df_apr)
        )

    df.show()

    print(f"df_oct shape: ({df_oct.count()}, {len(df_oct.columns)})")
   
    time_2 = time.time() - time_1

    print(f"\033[1;34m⏱️ Time taken for upload: {time_2:.2f} seconds\033[0m")
except Exception as e:
    print(f"\033[1;31m❌ An error occurred: {e}\033[0m")   

