import traceback

from check_new_data         import check_new_data
from clean_local_folders    import clean_local_folders
from grab_data              import grab_data
from dump_to_minio          import dump_to_minio
from dump_to_minio_spark    import dump_to_minio_spark
from load_data_from_minio   import load_data_from_minio
from clean_data             import clean_data
from dump_to_warehouse      import dump_to_warehouse
from cluster_data           import cluster_data
from update_processed_json  import update_processed_json






def process_month(year, month):
    print(f"📦 Processing data for {year}-{month:02}")
    clean_local_folders()
    grab_data(year, month)
    dump_to_minio_spark(year, month)
    clean_data(year, month)
    cluster_data(year, month, 4)
    dump_to_warehouse(year, month)
    update_processed_json(year, month)


def process_data():
    print("🔍 Checking for new data to process...")
    months_to_process = check_new_data()

    if not months_to_process:
        print("✅ No new data to process.")
        return

    for year, month in months_to_process:
        try:
            process_month(year, month)
        except Exception as e:
            print(f"\033[1;31m❌ Failed to process {year}-{month:02}: {e}\033[0m")
            traceback.print_exc()



