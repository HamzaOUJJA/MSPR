import os



def clean_local_folders():
    folders_to_clean = ['../data/raw', '../data/cleaned', '../data/clustered']
    for folder_path in folders_to_clean:
        for file_name in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file_name)
            os.remove(file_path) # This will raise an error if file_path is a directory
        print(f"Cleaned files in: {folder_path}")

    print("\033[1;32m ######## All specified local folders cleaned!\033[0m")