import traceback
from datetime           import datetime
from process_data       import process_data




if __name__ == "__main__":
    today = datetime.today()
    
    # Only run on the 1st of the month
    if today.day == 1:
        print(f"🕒 Running scheduled monthly job for {today.strftime('%Y-%m')}")
        try:
            process_data()
        except Exception as e:
            print(f"\033[1;31m🚨 Fatal error: {e}\033[0m")
            traceback.print_exc()
    else:
        print("⏭️ Not the 1st of the month. Skipping ETL run.")

# crontab -e
# 0 7 * * * /path/to/your/venv/bin/python /path/to/main.py >> /path/to/logs/monthly_etl.log 2>&1



