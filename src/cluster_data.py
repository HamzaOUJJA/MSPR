import os
import calendar
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.model_selection import GridSearchCV, cross_val_score, KFold
from sklearn.metrics import silhouette_score
from sklearn.utils import resample
from xgboost import XGBClassifier

# Optionnel : couleurs dans le terminal
try:
    from colorama import Fore, init
    init(autoreset=True)
except ImportError:
    class Fore:
        GREEN = CYAN = RED = YELLOW = BLUE = MAGENTA = RESET = ''
    print("ℹ️ Colorama non installé : les couleurs ne s’afficheront pas.")

def plot_elbow_curve(X_scaled, max_k=10, save_path=None):
    inertias = []
    for k in range(2, max_k + 1):
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(X_scaled)
        inertias.append(kmeans.inertia_)
    plt.figure(figsize=(8, 6))
    plt.plot(range(2, max_k + 1), inertias, marker='o')
    plt.title("Méthode du coude – Choix optimal de k")
    plt.xlabel("Nombre de clusters (k)")
    plt.ylabel("Inertie (variance intra-cluster)")
    plt.grid(True)
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path)
        print(f"{Fore.GREEN}✅ Courbe du coude enregistrée dans : {save_path}")
    else:
        plt.show()

def cluster_data(year: int, month: int, n_clusters: int = 5):
    print(f"{Fore.CYAN}📥 Lecture du fichier...")

    month_abbr = calendar.month_abbr[month]
    input_path = os.path.join("..", "data", "cleaned", f"cleaned_{year}-{month_abbr}.csv.gz")
    output_path = os.path.join("..", "data", "clustered", f"clustered_{year}-{month_abbr}.csv.gz")
    elbow_path = os.path.join("..", "data", "clustered", f"elbow_curve_{year}-{month_abbr}.png")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if not os.path.exists(input_path):
        print(f"{Fore.RED}❌ Fichier introuvable : {input_path}")
        return

    try:
        df = pd.read_csv(input_path, compression="gzip")
    except Exception as e:
        print(f"{Fore.RED}❌ Erreur de lecture : {e}")
        return

    print(f"{Fore.YELLOW}✅ Fichier chargé ({len(df)} lignes)")

    df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
    df = df.dropna(subset=['event_time'])
    if df.empty:
        print(f"{Fore.RED}❌ Aucune donnée après nettoyage des dates.")
        return

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

    activity = activity[['user_id', 'views', 'carts', 'remove_from_carts',
                         'purchases', 'user_session', 'price']]

    activity.columns = ['user_id', 'views', 'carts', 'removals', 'purchases',
                        'sessions', 'total_spent']

    merged = pd.merge(rfm, activity, on='user_id', how='inner').fillna(0)
    if merged.empty:
        print(f"{Fore.RED}❌ Fusion vide. Aucune donnée utilisateur.")
        return

    features = ['views', 'carts', 'removals', 'purchases', 'sessions', 'total_spent']
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(merged[features])

    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    print(f"{Fore.BLUE}📉 PCA appliqué : variance expliquée = {np.sum(pca.explained_variance_ratio_):.2%}")

    plot_elbow_curve(X_pca, max_k=10, save_path=elbow_path)

    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    merged['cluster'] = kmeans.fit_predict(X_pca)

    X_sample, labels_sample = resample(X_pca, merged['cluster'], n_samples=10000, random_state=42)
    silhouette = silhouette_score(X_sample, labels_sample)
    print(f"{Fore.MAGENTA}📐 Silhouette Score : {silhouette:.3f}")

    cluster_names = {
        0: "Occasional Spender",
        1: "Window Shopper",
        2: "Loyal Customer",
        3: "Heavy Cart User",
        4: "Top Spender"
    }
    merged['cluster_name'] = merged['cluster'].map(cluster_names).fillna("Other")

    final_df = merged[['user_id', 'views', 'carts', 'removals', 'purchases',
                       'sessions', 'total_spent', 'cluster', 'cluster_name']].copy()

    final_df.to_csv(output_path, index=False, compression='gzip')
    print(f"{Fore.GREEN}✅ Fichier sauvegardé : {output_path}")

    print(f"{Fore.BLUE}📊 Entraînement de XGBoost pour prédire les segments...")

    X = merged[features]
    y = merged['cluster']

    params = {
        'max_depth': [3, 5],
        'learning_rate': [0.1, 0.01],
        'n_estimators': [50, 100]
    }

    model = XGBClassifier(eval_metric='mlogloss')  # 🔧 Suppression de use_label_encoder
    grid = GridSearchCV(estimator=model, param_grid=params, cv=3, scoring='accuracy')
    grid.fit(X, y)

    print(f"{Fore.CYAN}🔎 Meilleurs paramètres trouvés : {grid.best_params_}")

    best_model = grid.best_estimator_
    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(best_model, X, y, cv=cv, scoring='accuracy')
    print(f"{Fore.CYAN}✅ Moyenne score cross-validation : {np.mean(scores):.3f}")

def predire_segment_nouveau_client(nouveau_client: dict, modele, scaler, cluster_labels: dict):
    features = ['views', 'carts', 'removals', 'purchases', 'sessions', 'total_spent']
    for key in features:
        if key not in nouveau_client:
            raise ValueError(f"❌ Clé manquante dans le dictionnaire : {key}")
    X_new = np.array([[nouveau_client[feat] for feat in features]])
    X_new_scaled = scaler.transform(X_new)
    cluster = modele.predict(X_new_scaled)[0]
    segment = cluster_labels.get(cluster, "Segment Inconnu")
    print(f"\n{Fore.GREEN}✅ Le client appartient au cluster n°{cluster} : « {segment} »")
    return cluster, segment

if __name__ == "__main__":
    year = 2019
    month = 10
    cluster_data(year, month)
