"""Utilitaires pour charger les données depuis MinIO Gold ou via l'API MongoDB"""

from io import BytesIO
import os
import sys
import time
from typing import Dict

import pandas as pd
import requests
from minio.error import S3Error

# Ajouter le chemin parent pour importer config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from flows.config import BUCKET_GOLD, get_minio_client


API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


def load_parquet_from_gold(object_path: str) -> pd.DataFrame:
    """Charge un fichier Parquet depuis MinIO Gold."""
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


# === Chargement direct depuis GOLD (MinIO) ===

def load_all_kpis() -> dict:
    """Charge tous les KPIs depuis Gold (MinIO direct)."""
    return {
        "globaux": load_parquet_from_gold("kpis/kpi_globaux.parquet"),
        "croissance": load_parquet_from_gold("kpis/kpi_croissance_mensuelle.parquet"),
        "rfm": load_parquet_from_gold("kpis/kpi_rfm.parquet"),
        "clv_detail": load_parquet_from_gold("kpis/kpi_clv_detail.parquet"),
        "clv_pays": load_parquet_from_gold("kpis/kpi_clv_pays.parquet"),
        "retention_global": load_parquet_from_gold("kpis/kpi_retention_global.parquet"),
        "retention_summary": load_parquet_from_gold("kpis/kpi_retention_summary.parquet"),
        "produits": load_parquet_from_gold("kpis/kpi_produits.parquet"),
        "top_produits_ca": load_parquet_from_gold("kpis/kpi_top_produits_ca.parquet"),
    }


def load_all_facts() -> dict:
    """Charge toutes les tables de faits depuis Gold (MinIO direct)."""
    return {
        "ca_jour": load_parquet_from_gold("facts/fact_ca_jour.parquet"),
        "ca_semaine": load_parquet_from_gold("facts/fact_ca_semaine.parquet"),
        "ca_mois": load_parquet_from_gold("facts/fact_ca_mois.parquet"),
        "ca_heure": load_parquet_from_gold("facts/fact_ca_heure.parquet"),
        "ca_pays": load_parquet_from_gold("facts/fact_ca_pays.parquet"),
    }


def load_all_analytics() -> dict:
    """Charge toutes les analyses depuis Gold (MinIO direct)."""
    return {
        "saisonnalite_jour": load_parquet_from_gold(
            "analytics/analytics_saisonnalite_jour.parquet"
        ),
        "saisonnalite_heure": load_parquet_from_gold(
            "analytics/analytics_saisonnalite_heure.parquet"
        ),
        "saisonnalite_mois": load_parquet_from_gold(
            "analytics/analytics_saisonnalite_mois.parquet"
        ),
        "concentration_summary": load_parquet_from_gold(
            "analytics/analytics_concentration_summary.parquet"
        ),
        "cohortes_total": load_parquet_from_gold(
            "analytics/analytics_cohortes_total.parquet"
        ),
    }


# === Chargement via l'API MongoDB ===

KPI_KEYS: Dict[str, str] = {
    "globaux": "globaux",
    "croissance": "croissance",
    "rfm": "rfm",
    "clv_detail": "clv_detail",
    "clv_pays": "clv_pays",
    "retention_global": "retention_global",
    "retention_summary": "retention_summary",
    "produits": "produits",
    "top_produits_ca": "top_produits_ca",
}


FACT_KEYS: Dict[str, str] = {
    "ca_jour": "ca_jour",
    "ca_semaine": "ca_semaine",
    "ca_mois": "ca_mois",
    "ca_heure": "ca_heure",
    "ca_pays": "ca_pays",
}


ANALYTICS_KEYS: Dict[str, str] = {
    "saisonnalite_jour": "saisonnalite_jour",
    "saisonnalite_heure": "saisonnalite_heure",
    "saisonnalite_mois": "saisonnalite_mois",
    "concentration_summary": "concentration_summary",
    "cohortes_total": "cohortes_total",
}


def _load_df_from_api(endpoint: str) -> pd.DataFrame:
    url = f"{API_BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Erreur lors de l'appel API {url}: {e}")
        return pd.DataFrame()


def load_all_kpis_api() -> dict:
    """Charge tous les KPIs via l'API MongoDB."""
    return {
        key: _load_df_from_api(f"/kpis/{api_name}")
        for key, api_name in KPI_KEYS.items()
    }


def load_all_facts_api() -> dict:
    """Charge toutes les tables de faits via l'API MongoDB."""
    return {
        key: _load_df_from_api(f"/facts/{api_name}")
        for key, api_name in FACT_KEYS.items()
    }


def load_all_analytics_api() -> dict:
    """Charge toutes les analyses via l'API MongoDB."""
    return {
        key: _load_df_from_api(f"/analytics/{api_name}")
        for key, api_name in ANALYTICS_KEYS.items()
    }


# === Benchmark des deux sources pour le dashboard ===

def benchmark_sources() -> Dict[str, float]:
    """
    Compare les temps de chargement entre MinIO direct et API Mongo.

    Retourne un dict avec:
        - minio_total: temps (s) pour charger kpis+facts+analytics depuis MinIO
        - api_total  : temps (s) pour charger kpis+facts+analytics via l'API
    """
    results: Dict[str, float] = {}

    t0 = time.perf_counter()
    _ = load_all_kpis()
    _ = load_all_facts()
    _ = load_all_analytics()
    results["minio_total"] = time.perf_counter() - t0

    t0 = time.perf_counter()
    _ = load_all_kpis_api()
    _ = load_all_facts_api()
    _ = load_all_analytics_api()
    results["api_total"] = time.perf_counter() - t0

    return results

