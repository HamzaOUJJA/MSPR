import requests
import os
import calendar

def grab_data(year: int, month: int):
    # Convert numeric month to abbreviated month name (e.g., 10 → 'Oct')
    month_abbr = calendar.month_abbr[month]  # This returns 'Jan', 'Feb', etc.
    file_name = f"{year}-{month_abbr}.csv.gz"
    url = f"https://data.rees46.com/datasets/marketplace/{file_name}"

    save_dir = os.path.join("..", "data","raw")
    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, file_name)

    try:
        print(f"\033[1;32mDownloading: {file_name}\033[0m")
        response = requests.get(url, stream=True)

        if response.status_code == 200:
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=256 * 1024):
                    file.write(chunk)
                    downloaded_size += len(chunk)
                    percentage = (downloaded_size / total_size) * 100
                    print(f"\r\033[38;5;214mSaving to {file_path} : {percentage:.2f}%\033[0m", end="")
            print("\n\033[1;32mDownload complete!\033[0m")
        else:
            print(f"\033[1;31mFailed to download. Status code: {response.status_code}\033[0m")
    except Exception as e:
        print("\033[1;31mError while downloading:\033[0m", e)

