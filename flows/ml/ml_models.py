"""Modèles ML pour enrichir automatiquement les données"""

import pandas as pd
import numpy as np
from typing import Dict, Optional
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from .feature_engineering import extract_features_auto
import warnings
warnings.filterwarnings('ignore')


def detect_anomalies_ml(df: pd.DataFrame, contamination: float = 0.1) -> pd.DataFrame:
    """
    Détecte les anomalies avec Isolation Forest (ML).
    
    Args:
        df: DataFrame avec features numériques
        contamination: Proportion attendue d'anomalies (0.1 = 10%)
    
    Returns:
        DataFrame avec colonne 'is_anomaly_ml'
    """
    if df.empty:
        return df
    
    df_result = df.copy()
    numeric_cols = df_result.select_dtypes(include=[np.number]).columns.tolist()
    
    if len(numeric_cols) < 2:
        print("⚠️ Pas assez de colonnes numériques pour la détection d'anomalies ML")
        df_result['is_anomaly_ml'] = False
        return df_result
    
    # Préparer les données
    X = df_result[numeric_cols].fillna(0)
    
    # Standardiser
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Isolation Forest
    iso_forest = IsolationForest(contamination=contamination, random_state=42)
    anomalies = iso_forest.fit_predict(X_scaled)
    
    df_result['is_anomaly_ml'] = anomalies == -1
    df_result['anomaly_score'] = iso_forest.score_samples(X_scaled)
    
    n_anomalies = df_result['is_anomaly_ml'].sum()
    print(f"🔍 ML: {n_anomalies} anomalie(s) détectée(s) ({n_anomalies/len(df_result)*100:.2f}%)")
    
    return df_result


def cluster_data(df: pd.DataFrame, n_clusters: Optional[int] = None) -> pd.DataFrame:
    """
    Clustering automatique avec K-Means pour segmenter les données.
    
    Args:
        df: DataFrame avec features
        n_clusters: Nombre de clusters (auto si None, basé sur sqrt(n/2))
    
    Returns:
        DataFrame avec colonne 'ml_cluster'
    """
    if df.empty:
        return df
    
    df_result = df.copy()
    numeric_cols = df_result.select_dtypes(include=[np.number]).columns.tolist()
    
    if len(numeric_cols) < 2:
        print("⚠️ Pas assez de colonnes numériques pour le clustering")
        df_result['ml_cluster'] = 0
        return df_result
    
    # Déterminer le nombre de clusters
    if n_clusters is None:
        n_clusters = max(2, int(np.sqrt(len(df_result) / 2)))
        n_clusters = min(n_clusters, 10)  # Max 10 clusters
    
    # Préparer les données
    X = df_result[numeric_cols].fillna(0)
    
    # Standardiser
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Réduction de dimension si trop de features
    if X_scaled.shape[1] > 10:
        pca = PCA(n_components=10, random_state=42)
        X_scaled = pca.fit_transform(X_scaled)
        print(f"📊 PCA appliqué: {X_scaled.shape[1]} composantes principales")
    
    # K-Means
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(X_scaled)
    
    df_result['ml_cluster'] = clusters
    df_result['ml_cluster_distance'] = kmeans.transform(X_scaled).min(axis=1)
    
    print(f"🎯 ML: {n_clusters} cluster(s) créé(s)")
    for i in range(n_clusters):
        count = (df_result['ml_cluster'] == i).sum()
        print(f"   Cluster {i}: {count} éléments ({count/len(df_result)*100:.1f}%)")
    
    return df_result


def predict_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prédit des scores basés sur les patterns détectés.
    Crée un score composite pour chaque ligne.
    
    Args:
        df: DataFrame avec features
    
    Returns:
        DataFrame avec colonne 'ml_score'
    """
    if df.empty:
        return df
    
    df_result = df.copy()
    numeric_cols = df_result.select_dtypes(include=[np.number]).columns.tolist()
    
    if len(numeric_cols) == 0:
        df_result['ml_score'] = 0.5
        return df_result
    
    # Score composite basé sur plusieurs métriques
    scores = []
    
    for col in numeric_cols[:5]:  # Limiter à 5 colonnes pour éviter la surcharge
        # Normaliser entre 0 et 1
        col_min = df_result[col].min()
        col_max = df_result[col].max()
        if col_max > col_min:
            normalized = (df_result[col] - col_min) / (col_max - col_min)
            scores.append(normalized)
    
    if scores:
        # Score moyen (peut être remplacé par un modèle plus sophistiqué)
        df_result['ml_score'] = pd.concat(scores, axis=1).mean(axis=1)
    else:
        df_result['ml_score'] = 0.5
    
    # Normaliser le score final entre 0 et 100
    df_result['ml_score'] = (df_result['ml_score'] - df_result['ml_score'].min()) / \
                            (df_result['ml_score'].max() - df_result['ml_score'].min() + 1e-6) * 100
    
    print(f"📈 ML: Scores prédits (moyenne: {df_result['ml_score'].mean():.2f})")
    
    return df_result


def enrich_with_ml(df: pd.DataFrame, dataset_name: str = "dataset") -> pd.DataFrame:
    """
    Enrichit un DataFrame avec toutes les prédictions ML.
    Pipeline complet : features → anomalies → clustering → scores.
    
    Args:
        df: DataFrame source depuis Silver
        dataset_name: Nom du dataset
    
    Returns:
        DataFrame enrichi avec toutes les colonnes ML
    """
    if df.empty:
        return df
    
    print(f"\n🤖 Enrichissement ML pour {dataset_name}...")
    
    # 1. Feature engineering
    df_enriched = extract_features_auto(df, dataset_name)
    
    # 2. Détection d'anomalies
    df_enriched = detect_anomalies_ml(df_enriched)
    
    # 3. Clustering
    df_enriched = cluster_data(df_enriched)
    
    # 4. Scores prédits
    df_enriched = predict_scores(df_enriched)
    
    print(f"✅ Enrichissement ML terminé: {len(df_enriched.columns)} colonnes totales")
    
    return df_enriched

