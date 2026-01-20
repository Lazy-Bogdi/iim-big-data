"""Fonctions de nettoyage des données pour la couche Silver"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from datetime import datetime


def handle_missing_values(
    df: pd.DataFrame,
    strategy: str = "drop",
    columns: Optional[List[str]] = None,
    fill_value: Optional[any] = None
) -> pd.DataFrame:
    """
    Gère les valeurs manquantes selon une stratégie.
    
    Args:
        df: DataFrame à nettoyer
        strategy: "drop" (supprimer), "fill" (remplacer), "forward_fill"
        columns: Colonnes spécifiques à traiter (None = toutes)
        fill_value: Valeur de remplacement si strategy="fill"
    
    Returns:
        DataFrame nettoyé
    """
    df_clean = df.copy()
    
    if columns is None:
        columns = df_clean.columns.tolist()
    
    if strategy == "drop":
        df_clean = df_clean.dropna(subset=columns)
    elif strategy == "fill":
        if fill_value is not None:
            df_clean[columns] = df_clean[columns].fillna(fill_value)
        else:
            # Remplissage par la moyenne pour numériques, mode pour catégorielles
            for col in columns:
                if df_clean[col].dtype in ['int64', 'float64']:
                    df_clean[col].fillna(df_clean[col].mean(), inplace=True)
                else:
                    df_clean[col].fillna(df_clean[col].mode()[0] if len(df_clean[col].mode()) > 0 else "UNKNOWN", inplace=True)
    elif strategy == "forward_fill":
        df_clean[columns] = df_clean[columns].ffill()
    
    return df_clean


def standardize_dates(
    df: pd.DataFrame,
    date_columns: List[str],
    target_format: str = "%Y-%m-%d"
) -> pd.DataFrame:
    """
    Standardise les formats de dates.
    
    Args:
        df: DataFrame contenant les colonnes de dates
        date_columns: Liste des colonnes de dates à standardiser
        target_format: Format cible (pour affichage)
    
    Returns:
        DataFrame avec dates standardisées
    """
    df_clean = df.copy()
    
    for col in date_columns:
        if col in df_clean.columns:
            # Convertir en datetime
            df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce', infer_datetime_format=True)
            
            # Supprimer les dates futures (anomalies)
            today = pd.Timestamp.now()
            df_clean = df_clean[df_clean[col] <= today]
    
    return df_clean


def normalize_data_types(
    df: pd.DataFrame,
    schema: Dict[str, str]
) -> pd.DataFrame:
    """
    Normalise les types de données selon un schéma.
    
    Args:
        df: DataFrame à normaliser
        schema: Dict {colonne: type} ex: {"id": "int64", "montant": "float64"}
    
    Returns:
        DataFrame avec types normalisés
    """
    df_clean = df.copy()
    
    for col, dtype in schema.items():
        if col in df_clean.columns:
            try:
                if dtype == "string":
                    df_clean[col] = df_clean[col].astype("string")
                elif dtype == "datetime64[ns]":
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                else:
                    df_clean[col] = df_clean[col].astype(dtype)
            except Exception as e:
                print(f"Warning: Impossible de convertir {col} en {dtype}: {e}")
    
    return df_clean


def remove_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: str = "first"
) -> pd.DataFrame:
    """
    Supprime les doublons.
    
    Args:
        df: DataFrame à dédupliquer
        subset: Colonnes à considérer pour la détection (None = toutes)
        keep: "first" (garder premier), "last" (garder dernier), False (supprimer tous)
    
    Returns:
        DataFrame dédupliqué
    """
    df_clean = df.copy()
    
    initial_count = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=subset, keep=keep)
    removed_count = initial_count - len(df_clean)
    
    if removed_count > 0:
        print(f"⚠️  {removed_count} doublon(s) supprimé(s)")
    
    return df_clean


def clean_clients_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie spécifiquement les données clients.
    
    Args:
        df: DataFrame clients brut
    
    Returns:
        DataFrame clients nettoyé
    """
    print(f"📊 Nettoyage clients: {len(df)} enregistrements initiaux")
    
    # 1. Normaliser les types
    schema = {
        "id_client": "int64",
        "nom": "string",
        "email": "string",
        "date_inscription": "datetime64[ns]",
        "pays": "string"
    }
    df_clean = normalize_data_types(df, schema)
    
    # 2. Supprimer les doublons sur id_client (clé primaire)
    df_clean = remove_duplicates(df_clean, subset=["id_client"], keep="first")
    
    # 3. Supprimer les doublons sur email (doit être unique)
    df_clean = remove_duplicates(df_clean, subset=["email"], keep="first")
    
    # 4. Standardiser les dates
    df_clean = standardize_dates(df_clean, date_columns=["date_inscription"])
    
    # 5. Gérer les valeurs nulles
    # Supprimer les lignes avec nom ou email manquant (critique)
    df_clean = handle_missing_values(
        df_clean,
        strategy="drop",
        columns=["nom", "email"]
    )
    
    # Remplacer les pays manquants par "UNKNOWN"
    df_clean = handle_missing_values(
        df_clean,
        strategy="fill",
        columns=["pays"],
        fill_value="UNKNOWN"
    )
    
    # 6. Validation email basique
    df_clean = df_clean[df_clean["email"].str.contains("@", na=False)]
    
    print(f"✅ Nettoyage clients terminé: {len(df_clean)} enregistrements valides")
    
    return df_clean


def clean_achats_data(df: pd.DataFrame, valid_client_ids: Optional[pd.Series] = None) -> pd.DataFrame:
    """
    Nettoie spécifiquement les données achats.
    
    Args:
        df: DataFrame achats brut
        valid_client_ids: Series des id_client valides (pour intégrité référentielle)
    
    Returns:
        DataFrame achats nettoyé
    """
    print(f"📊 Nettoyage achats: {len(df)} enregistrements initiaux")
    
    # 1. Normaliser les types
    schema = {
        "id_achat": "int64",
        "id_client": "int64",
        "date_achat": "datetime64[ns]",
        "montant": "float64",
        "produit": "string"
    }
    df_clean = normalize_data_types(df, schema)
    
    # 2. Supprimer les doublons sur id_achat (clé primaire)
    df_clean = remove_duplicates(df_clean, subset=["id_achat"], keep="first")
    
    # 3. Standardiser les dates
    df_clean = standardize_dates(df_clean, date_columns=["date_achat"])
    
    # 4. Supprimer les valeurs nulles critiques
    df_clean = handle_missing_values(
        df_clean,
        strategy="drop",
        columns=["id_client", "date_achat", "montant"]
    )
    
    # 5. Supprimer les montants aberrants (négatifs ou trop élevés)
    df_clean = df_clean[(df_clean["montant"] > 0) & (df_clean["montant"] <= 10000)]
    
    # 6. Intégrité référentielle : vérifier que id_client existe dans clients
    if valid_client_ids is not None:
        initial_count = len(df_clean)
        df_clean = df_clean[df_clean["id_client"].isin(valid_client_ids)]
        removed_count = initial_count - len(df_clean)
        if removed_count > 0:
            print(f"⚠️  {removed_count} achat(s) supprimé(s) (id_client invalide)")
    
    # 7. Remplacer produit manquant par "UNKNOWN"
    df_clean = handle_missing_values(
        df_clean,
        strategy="fill",
        columns=["produit"],
        fill_value="UNKNOWN"
    )
    
    print(f"✅ Nettoyage achats terminé: {len(df_clean)} enregistrements valides")
    
    return df_clean

