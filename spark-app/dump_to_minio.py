########################################## 
###  This file uploads the data in ../data to MinIO
##########################################

from minio import Minio
import os


def dump_to_minio():
    print("\033[1;32m        ########    Uploading Data To MinIO!\033[0m")

    minioClient = Minio( # Client MinIO 
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    bucket = "mspr-minio-bucket"  
    # Create the bucket if it doesn't exist
    if not minioClient.bucket_exists(bucket):
        minioClient.make_bucket(bucket)
        print(f"Bucket '{bucket}' created.")
    
    clean_minio_bucket(minioClient, bucket) # Clean MinIO before uploading
    baseDir = os.path.abspath("../data") # Path to the folder containing the files

    try:
        for root, _, files in os.walk(baseDir):  # Iterate through all files in the folder and upload them # Ignore dirs by replacing it with '_'
            for file in files:
                file_path = os.path.join(root, file)
                object_name = os.path.relpath(file_path, baseDir)  # Preserve folder structure

                try:
                    # Upload the file to MinIO with progress tracking
                    minioClient.fput_object(bucket, object_name, file_path)
                    print(f"\033[38;5;214mUploaded To MinIO : {object_name}\033[0m")

                except Exception as e:
                    print(f"Unexpected error uploading {file_path}: {e}")
                    return 0
        return 1
    except Exception as e:
        print("\033[1;31m        ########    Problem Occured While Uploading Data To MinIO\033[0m")
        print(e)
        return 0



def clean_minio_bucket(minio_client, bucket):
    if minio_client.bucket_exists(bucket):
        objects = minio_client.list_objects(bucket, recursive=True)
        for obj in objects:
            minio_client.remove_object(bucket, obj.object_name)
        print(f"\033[1;32m        ########    Bucket '{bucket}' cleaned.\033[0m")