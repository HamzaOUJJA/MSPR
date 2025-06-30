import os
import calendar
import pandas as pd
from colorama import Fore, Style, init
import gzip


# Initialize colorama
init(autoreset=True)

def clean_data(y, m):
    chunksize = 100_000

    raw_data_dir = os.path.join("..", "data", "raw")
    file_path = os.path.join(raw_data_dir, f"{y}-{calendar.month_abbr[m]}.csv.gz")
    print(f"{Fore.CYAN}Reading CSV file...")

    # Step 2: Estimate price quantiles using a 100,000-row sample
    sample_df = pd.read_csv(file_path, compression="gzip", nrows=100_000)
    q1 = sample_df["price"].quantile(0.25)
    q3 = sample_df["price"].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Step 3: Prepare output
    base_output_dir = os.path.join("..", "data", "cleaned")
    os.makedirs(base_output_dir, exist_ok=True)
    output_file_name = f"cleaned_{y}-{calendar.month_abbr[m]}.csv.gz"
    output_file_path = os.path.join(base_output_dir, output_file_name)

    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    # Step 4: Count total number of lines (excluding header)
    with gzip.open(file_path, "rt") as f:
        total_lines = sum(1 for _ in f) - 1

    if total_lines <= 0:
        print(f"{Fore.RED}❌ ERROR: Total line count is 0. Cannot proceed.")
        return

    processed_lines = 0
    chunk_no = 1

    # Step 5: Process file in chunks
    for chunk in pd.read_csv(file_path, compression="gzip", chunksize=chunksize):
        df = chunk.copy()

        # Fill missing values
        df["brand"] = df["brand"].fillna("unknown")
        df["category_code"] = df["category_code"].fillna("unknown")

        # Drop rows with NA in critical columns
        df.dropna(subset=["user_id", "product_id", "user_session"], inplace=True)

        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Filter price values
        df = df[df["price"].notna() & (df["price"] >= 0)]
        df = df[(df["price"] >= lower_bound) & (df["price"] <= upper_bound)]

        # Parse datetime and extract parts
        df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
        df["event_year"] = df["event_time"].dt.year
        df["event_month"] = df["event_time"].dt.month

        # Clean string columns
        df["brand"] = df["brand"].str.strip().str.lower()
        df["category_code"] = df["category_code"].str.strip().str.lower()

        # Append cleaned chunk to output file
        df.to_csv(output_file_path, mode="a", index=False, compression="gzip", header=(chunk_no == 1))

        # Update progress with a single updating line
        processed_lines += len(chunk)
        progress = (processed_lines / total_lines) * 100
        print(f"\r{Fore.BLUE}Cleaning the data:  {progress:.2f}%", end="")

        chunk_no += 1

    print(f"\n{Fore.GREEN}✅ Done! {Style.BRIGHT}Cleaned data saved to: {Fore.YELLOW}{output_file_path}")
































def clean_data_no_chunks(y, m):
    raw_data_dir = os.path.join("..", "data", "raw")
    file_path = os.path.join(raw_data_dir, f"{y}-{calendar.month_abbr[m]}.csv.gz")
    print("Step 1: Reading CSV file...")

    df = pd.read_csv(file_path, compression="gzip")

    print("Step 2: Calculating price quantiles...")
    q1 = df["price"].quantile(0.25)
    q3 = df["price"].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    print("Step 3: Cleaning and transforming data...")
    df_clean = df.copy()

    # Fill missing brand/category_code with 'unknown'
    df_clean["brand"] = df_clean["brand"].fillna("unknown")
    df_clean["category_code"] = df_clean["category_code"].fillna("unknown")

    # Drop rows with any NA in critical columns
    df_clean.dropna(subset=["user_id", "product_id", "user_session"], inplace=True)

    # Remove duplicates
    df_clean.drop_duplicates(inplace=True)

    # Filter price conditions
    df_clean = df_clean[df_clean["price"].notna() & (df_clean["price"] >= 0)]
    df_clean = df_clean[(df_clean["price"] >= lower_bound) & (df_clean["price"] <= upper_bound)]

    # Parse datetime and extract year/month
    df_clean["event_time"] = pd.to_datetime(df_clean["event_time"], errors="coerce")
    df_clean["event_year"] = df_clean["event_time"].dt.year
    df_clean["event_month"] = df_clean["event_time"].dt.month

    # Clean text columns
    df_clean["brand"] = df_clean["brand"].str.strip().str.lower()
    df_clean["category_code"] = df_clean["category_code"].str.strip().str.lower()

    print("Step 4: Saving cleaned data...")

    base_output_dir = os.path.join("..", "data", "cleaned")
    os.makedirs(base_output_dir, exist_ok=True)

    final_output_file_name = f"cleaned_{y}-{calendar.month_abbr[m]}.csv.gz"
    final_output_file_path = os.path.join(base_output_dir, final_output_file_name)

    df_clean.to_csv(final_output_file_path, index=False, compression="gzip")
    print(f"✅ Cleaned data saved as: {final_output_file_path}")
