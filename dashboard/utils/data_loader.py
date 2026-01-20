"""Utilitaires pour charger les données depuis MinIO Gold"""

from io import BytesIO
import pandas as pd
from minio.error import S3Error
import sys
import os

# Ajouter le chemin parent pour importer config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from flows.config import BUCKET_GOLD, get_minio_client


def load_parquet_from_gold(object_path: str) -> pd.DataFrame:
    """
    Charge un fichier Parquet depuis MinIO Gold.
    
    Args:
        object_path: Chemin du fichier dans Gold (ex: "kpis/kpi_globaux.parquet")
    
    Returns:
        DataFrame pandas
    """
    client = get_minio_client()
    
    try:
        response = client.get_object(BUCKET_GOLD, object_path)
        df = pd.read_parquet(BytesIO(response.read()))
        response.close()
        response.release_conn()
        return df
    except S3Error as e:
        print(f"Erreur lors du chargement de {object_path}: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Erreur inattendue: {e}")
        return pd.DataFrame()


def load_all_kpis() -> dict:
    """Charge tous les KPIs depuis Gold"""
    return {
        'globaux': load_parquet_from_gold('kpis/kpi_globaux.parquet'),
        'croissance': load_parquet_from_gold('kpis/kpi_croissance_mensuelle.parquet'),
        'rfm': load_parquet_from_gold('kpis/kpi_rfm.parquet'),
        'clv_detail': load_parquet_from_gold('kpis/kpi_clv_detail.parquet'),
        'clv_pays': load_parquet_from_gold('kpis/kpi_clv_pays.parquet'),
        'retention_global': load_parquet_from_gold('kpis/kpi_retention_global.parquet'),
        'retention_summary': load_parquet_from_gold('kpis/kpi_retention_summary.parquet'),
        'produits': load_parquet_from_gold('kpis/kpi_produits.parquet'),
        'top_produits_ca': load_parquet_from_gold('kpis/kpi_top_produits_ca.parquet'),
    }


def load_all_facts() -> dict:
    """Charge toutes les tables de faits depuis Gold"""
    return {
        'ca_jour': load_parquet_from_gold('facts/fact_ca_jour.parquet'),
        'ca_semaine': load_parquet_from_gold('facts/fact_ca_semaine.parquet'),
        'ca_mois': load_parquet_from_gold('facts/fact_ca_mois.parquet'),
        'ca_heure': load_parquet_from_gold('facts/fact_ca_heure.parquet'),
        'ca_pays': load_parquet_from_gold('facts/fact_ca_pays.parquet'),
    }


def load_all_analytics() -> dict:
    """Charge toutes les analyses depuis Gold"""
    return {
        'saisonnalite_jour': load_parquet_from_gold('analytics/analytics_saisonnalite_jour.parquet'),
        'saisonnalite_heure': load_parquet_from_gold('analytics/analytics_saisonnalite_heure.parquet'),
        'saisonnalite_mois': load_parquet_from_gold('analytics/analytics_saisonnalite_mois.parquet'),
        'concentration_summary': load_parquet_from_gold('analytics/analytics_concentration_summary.parquet'),
        'cohortes_total': load_parquet_from_gold('analytics/analytics_cohortes_total.parquet'),
    }


