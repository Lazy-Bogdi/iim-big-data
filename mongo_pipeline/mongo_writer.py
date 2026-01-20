import time
from datetime import datetime
from io import BytesIO
from typing import Dict

import pandas as pd
from pymongo import MongoClient

from flows.config import BUCKET_GOLD, get_minio_client


KPI_SPECS: Dict[str, str] = {
    "globaux": "kpis/kpi_globaux.parquet",
    "croissance": "kpis/kpi_croissance_mensuelle.parquet",
    "rfm": "kpis/kpi_rfm.parquet",
    "clv_detail": "kpis/kpi_clv_detail.parquet",
    "clv_pays": "kpis/kpi_clv_pays.parquet",
    "retention_global": "kpis/kpi_retention_global.parquet",
    "retention_summary": "kpis/kpi_retention_summary.parquet",
    "produits": "kpis/kpi_produits.parquet",
    "top_produits_ca": "kpis/kpi_top_produits_ca.parquet",
}


FACT_SPECS: Dict[str, str] = {
    "ca_jour": "facts/fact_ca_jour.parquet",
    "ca_semaine": "facts/fact_ca_semaine.parquet",
    "ca_mois": "facts/fact_ca_mois.parquet",
    "ca_heure": "facts/fact_ca_heure.parquet",
    "ca_pays": "facts/fact_ca_pays.parquet",
}


ANALYTICS_SPECS: Dict[str, str] = {
    "saisonnalite_jour": "analytics/analytics_saisonnalite_jour.parquet",
    "saisonnalite_heure": "analytics/analytics_saisonnalite_heure.parquet",
    "saisonnalite_mois": "analytics/analytics_saisonnalite_mois.parquet",
    "concentration_summary": "analytics/analytics_concentration_summary.parquet",
    "cohortes_total": "analytics/analytics_cohortes_total.parquet",
}


def read_parquet_from_gold(object_path: str) -> pd.DataFrame:
    """Lit un Parquet dans le bucket Gold via MinIO et retourne un DataFrame."""
    client = get_minio_client()
    response = client.get_object(BUCKET_GOLD, object_path)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()

    if not data:
        return pd.DataFrame()

    return pd.read_parquet(BytesIO(data))


def get_mongo_client(
    uri: str = "mongodb://mongo:mongo@localhost:27017/?authSource=admin",
) -> MongoClient:
    """
    Retourne un client MongoDB authentifié sur la base admin.
    """
    return MongoClient(uri)


def sync_group(df_specs: Dict[str, str], group: str, db) -> Dict[str, int]:
    """
    Synchronise un groupe de tables Gold (kpis, facts, analytics) vers Mongo.

    Args:
        df_specs: mapping nom_logique -> chemin Parquet dans Gold
        group: "kpis", "facts" ou "analytics"
        db: instance de base MongoDB

    Returns:
        Dict des nombres de lignes insérées par collection
    """
    counts: Dict[str, int] = {}

    for name, path in df_specs.items():
        collection_name = f"{group}_{name}"
        print(f"🔄 Sync {group}/{name} depuis {path} vers collection {collection_name}...")

        df = read_parquet_from_gold(path)
        if df.empty:
            print(f"⚠️ Aucune donnée pour {path}, collection ignorée")
            continue

        # Remplacer les NaN / NaT par None pour compat MongoDB
        df_clean = df.where(pd.notnull(df), None)
        docs = df_clean.to_dict(orient="records")

        # Sécuriser encore au niveau dict (certains NaT peuvent subsister)
        for doc in docs:
            for key, value in list(doc.items()):
                if pd.isna(value):
                    doc[key] = None

        coll = db[collection_name]
        coll.drop()  # on remplace complètement le contenu
        if docs:
            coll.insert_many(docs)
        counts[collection_name] = len(docs)
        print(f"✅ {collection_name}: {len(docs)} documents insérés")

    return counts


def run_mongo_sync(
    mongo_uri: str = "mongodb://mongo:mongo@localhost:27017/?authSource=admin",
    db_name: str = "bigdata_analytics",
) -> Dict[str, Dict[str, int]]:
    """
    Pipeline complète : Gold (Parquet) -> MongoDB.

    - Lit toutes les tables Gold nécessaires au dashboard
    - Les écrit dans des collections Mongo (`kpis_*`, `facts_*`, `analytics_*`)
    - Enregistre un document de métadonnées avec le temps total de refresh
    """
    start = time.perf_counter()
    client = get_mongo_client(mongo_uri)
    db = client[db_name]

    print("🚀 Démarrage de la synchronisation Gold -> MongoDB...")

    results = {
        "kpis": sync_group(KPI_SPECS, "kpis", db),
        "facts": sync_group(FACT_SPECS, "facts", db),
        "analytics": sync_group(ANALYTICS_SPECS, "analytics", db),
    }

    duration = time.perf_counter() - start

    # Enregistrer les métadonnées
    metadata_coll = db["metadata_refresh"]
    doc = {
        "name": "gold_to_mongo_refresh",
        "refreshed_at": datetime.utcnow(),
        "duration_seconds": duration,
        "counts": results,
    }
    metadata_coll.insert_one(doc)

    print(f"⏱️ Refresh Gold -> Mongo terminé en {duration:.2f} s")
    return results


if __name__ == "__main__":
    run_mongo_sync()


