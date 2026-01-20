"""Feature engineering automatique pour le ML"""

import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import datetime


def extract_features_auto(df: pd.DataFrame, dataset_name: str = "dataset") -> pd.DataFrame:
    """
    Extrait automatiquement des features depuis n'importe quel DataFrame.
    Détecte les patterns et crée des features utiles pour le ML.
    
    Args:
        df: DataFrame source
        dataset_name: Nom du dataset
    
    Returns:
        DataFrame enrichi avec les nouvelles features
    """
    if df.empty:
        return df
    
    df_features = df.copy()
    
    print(f"🔧 Extraction de features pour {dataset_name}...")
    
    # 1. Features temporelles (si colonnes de dates détectées)
    date_cols = [col for col in df_features.columns 
                 if df_features[col].dtype == 'datetime64[ns]']
    
    for col in date_cols:
        df_features[f"{col}_year"] = df_features[col].dt.year
        df_features[f"{col}_month"] = df_features[col].dt.month
        df_features[f"{col}_day"] = df_features[col].dt.day
        df_features[f"{col}_dayofweek"] = df_features[col].dt.dayofweek
        df_features[f"{col}_is_weekend"] = df_features[col].dt.dayofweek.isin([5, 6])
        
        # Âge en jours depuis la date
        if col != 'date_achat':  # Éviter les calculs redondants
            df_features[f"{col}_days_since"] = (pd.Timestamp.now() - df_features[col]).dt.days
    
    # 2. Features numériques (statistiques)
    numeric_cols = df_features.select_dtypes(include=[np.number]).columns
    
    for col in numeric_cols:
        # Normalisation (z-score)
        mean_val = df_features[col].mean()
        std_val = df_features[col].std()
        if std_val > 0:
            df_features[f"{col}_normalized"] = (df_features[col] - mean_val) / std_val
        
        # Binning (catégorisation)
        if df_features[col].nunique() > 10:
            df_features[f"{col}_binned"] = pd.qcut(
                df_features[col], 
                q=5, 
                labels=['Very Low', 'Low', 'Medium', 'High', 'Very High'],
                duplicates='drop'
            )
    
    # 3. Features catégorielles (encodage)
    categorical_cols = df_features.select_dtypes(include=['object', 'string']).columns
    
    for col in categorical_cols:
        # Fréquence d'encodage (combien de fois cette valeur apparaît)
        value_counts = df_features[col].value_counts()
        df_features[f"{col}_frequency"] = df_features[col].map(value_counts)
        
        # Top N categories (garder seulement les top 10, le reste = "Other")
        if df_features[col].nunique() > 10:
            top_categories = value_counts.head(10).index
            df_features[f"{col}_top_category"] = df_features[col].apply(
                lambda x: x if x in top_categories else "Other"
            )
    
    # 4. Features d'interaction (si plusieurs colonnes numériques)
    if len(numeric_cols) >= 2:
        # Ratio entre les deux premières colonnes numériques
        col1, col2 = numeric_cols[0], numeric_cols[1]
        if (df_features[col2] != 0).any():
            df_features[f"{col1}_div_{col2}"] = df_features[col1] / (df_features[col2] + 1e-6)
    
    # 5. Features de comptage (si colonnes ID détectées)
    id_cols = [col for col in df_features.columns if 'id' in col.lower()]
    for id_col in id_cols:
        # Nombre d'occurrences de cet ID
        id_counts = df_features[id_col].value_counts()
        df_features[f"{id_col}_count"] = df_features[id_col].map(id_counts)
    
    print(f"✅ {len(df_features.columns) - len(df.columns)} nouvelles features créées")
    
    return df_features

