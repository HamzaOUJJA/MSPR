import os
import calendar
from minio import Minio

def load_data_from_minio(year: int, month: int):
    """
    Downloads a specific file from MinIO using year and month.
    Example: 2019, 10 → downloads '2019-Oct.csv.gz'
    """
    print("\033[1;32m######## Downloading Data From MinIO!\033[0m")
    try:
        # Convert month number to 3-letter abbreviation
        month_abbr = calendar.month_abbr[month]
        file_name = f"{year}-{month_abbr}.csv.gz"

        # Initialize MinIO client
        minio_client = Minio(
            "localhost:9000",
            secure=False,
            access_key="minio",
            secret_key="minio123"
        )

        # Define bucket and local save path
        bucket = "mspr-minio-bucket"
        download_dir = os.path.join("..", "data", "raw")
        file_path = os.path.join(download_dir, file_name)

        # Ensure bucket exists
        if not minio_client.bucket_exists(bucket):
            print(f"\033[1;31mBucket '{bucket}' does not exist.\033[0m")
            return 0

        # Ensure download directory exists
        os.makedirs(download_dir, exist_ok=True)

        # Get object from MinIO
        response = minio_client.get_object(bucket, file_name)
        file_size = int(response.headers.get("Content-Length", 0))

        # Download in chunks and show progress
        downloaded = 0
        chunk_size = 1024 * 1024
        with open(file_path, 'wb') as f:
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                progress = (downloaded / file_size) * 100
                print(f"\r\033[38;5;214mDownloading {file_name} : {progress:.2f}%\033[0m", end="")

        print(f"\n\033[1;32m✔️ Successfully downloaded {file_name} to {file_path}\033[0m")
        return 1

    except Exception as e:
        print("\n\033[1;31m######## Problem occurred while downloading data from MinIO\033[0m")
        print(e)
        return 0
