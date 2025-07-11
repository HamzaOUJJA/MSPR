import os
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta

def check_new_data():
    processed_json_path="../data/processed_months.json"

    # Load processed months if the file exists
    if os.path.exists(processed_json_path):
        with open(processed_json_path, "r") as f:
            processed_months = set(tuple(x) for x in json.load(f))
    else:
        processed_months = set()

    # Get the last 10 months from today
    today = datetime.today()
    recent_months = []
    for i in range(10):
        date = today - relativedelta(months=i)
        recent_months.append((date.year, date.month))

    # Filter to only keep the months not already processed
    months_to_process = [m for m in recent_months if m not in processed_months]

    return months_to_process
