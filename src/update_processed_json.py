import os
import calendar
import json



def update_processed_json(year: int, month: int ):
    path = "../data/processed_months.json"

    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        with open(path, "r") as f:
            months = json.load(f)
    else:
        months = []

    month_str = f"{year}-{calendar.month_abbr[month]}"
    if month_str not in months:
        months.append(month_str)
        with open(path, "w") as f:
            json.dump(months, f, indent=2)
