import os
import sys
import calendar
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.cluster import KMeans
import xgboost as xgb
from colorama import Fore, init
import gzip
import warnings
import contextlib
init(autoreset=True)


@contextlib.contextmanager
def suppress_stderr():
    with open(os.devnull, "w") as fnull:
        old_stderr = sys.stderr
        sys.stderr = fnull
        try:
            yield
        finally:
            sys.stderr = old_stderr

def cluster_data(year: int, month: int, n_components: int = 2, n_clusters: int = 5):
    chunksize = 100_000

    print(f"{Fore.CYAN}Reading CSV file...")

    month_abbr = calendar.month_abbr[month]
    input_path = os.path.join("..", "data", "cleaned", f"cleaned_{year}-{month_abbr}.csv.gz")
    output_path = os.path.join("..", "data", "clustered", f"clustered_{year}-{month_abbr}.csv.gz")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if not os.path.exists(input_path):
        return

    # Count total lines
    with gzip.open(input_path, "rt") as f:
        total_lines = sum(1 for _ in f) - 1  # exclude header

    with gzip.open(input_path, "rt") as f_in, gzip.open(output_path, "wt") as f_out:
        reader = pd.read_csv(f_in, chunksize=chunksize)
        header_written = False
        processed_lines = 0

        for chunk in reader:
            df = chunk.copy()
            processed_lines += len(df)
            progress = (processed_lines / total_lines) * 100
            sys.stdout.write(f"\rClustering the data: {progress:.2f}%")
            sys.stdout.flush()

            df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
            df = df.dropna(subset=['event_time'])
            if df.empty:
                continue
            current_date = df['event_time'].max()

            rfm = df.groupby('user_id').agg({
                'event_time': lambda x: (current_date - x.max()).days,
                'user_session': pd.Series.nunique,
                'price': 'sum'
            }).reset_index()
            rfm.columns = ['user_id', 'recency', 'frequency', 'monetary']

            activity = df.groupby('user_id').agg({
                'event_type': list,
                'price': 'sum',
                'user_session': pd.Series.nunique
            }).reset_index()

            for etype in ['view', 'cart', 'remove_from_cart', 'purchase']:
                activity[etype + 's'] = activity['event_type'].apply(lambda x: x.count(etype))

            activity = activity[['user_id', 'views', 'carts', 'remove_from_carts', 'purchases', 'user_session', 'price']]
            activity.columns = ['user_id', 'views', 'carts', 'removals', 'purchases', 'sessions', 'total_spent']

            merged = pd.merge(rfm, activity, on='user_id', how='inner').fillna(0)
            if merged.empty:
                continue

            features = ["views", "carts", "removals", "purchases", "sessions", "total_spent", "recency", "frequency", "monetary"]
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(merged[features])

            pca = PCA(n_components=n_components)
            X_pca = pca.fit_transform(X_scaled)
            merged['pca1'] = X_pca[:, 0]
            if n_components > 1:
                merged['pca2'] = X_pca[:, 1]

            merged['high_spender'] = (merged['monetary'] > 1000).astype(int)
            X = merged[['pca1'] + (['pca2'] if n_components > 1 else [])]
            y = merged['high_spender']

            with suppress_stderr(), warnings.catch_warnings():
                warnings.simplefilter("ignore")
                model = xgb.XGBClassifier(eval_metric="logloss")
                model.fit(X, y)

            kmeans = KMeans(n_clusters=n_clusters, random_state=42)
            merged['cluster'] = kmeans.fit_predict(X)

            cluster_names = {
                0: "Occasional Spender",
                1: "Window Shopper",
                2: "Loyal Customer",
                3: "Heavy Cart User",
                4: "Top Spender"
            }
            merged['cluster_name'] = merged['cluster'].map(cluster_names).fillna("Other")

            cluster_map = dict(zip(merged['user_id'], merged['cluster_name']))
            df['cluster'] = df['user_id'].map(cluster_map).fillna("Unknown")

            df.to_csv(f_out, index=False, header=not header_written)
            f_out.flush()  # <--- forcer l’écriture sur disque
            header_written = True

    sys.stdout.write("\rClustering the data: 100.00%\n")
