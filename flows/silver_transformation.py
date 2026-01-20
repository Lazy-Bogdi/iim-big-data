"""Flow de transformation Silver : nettoyage et normalisation des données"""

from io import BytesIO
from typing import Dict, Any

import pandas as pd
from prefect import flow, task
from minio.error import S3Error

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client
from transformations.data_cleaning import clean_clients_data, clean_achats_data, clean_data_generic
from transformations.quality_checks import validate_data_quality, generate_quality_report


# Règles de qualité
QUALITY_RULES = {
    "clients": {
        "completeness": {
            "nom": 0.95,
            "email": 0.95,
            "pays": 0.90
        },
        "uniqueness": {
            "id_client": 1.0,
            "email": 0.99
        },
        "validity": {
            "date_inscription": "not_future",
            "email": "email_format"
        }
    },
    "achats": {
        "completeness": {
            "montant": 1.0,
            "date_achat": 1.0
        },
        "validity": {
            "montant": {"min": 0, "max": 10000},
            "date_achat": "not_future"
        }
    }
}


@task(name="read_bronze_csv", retries=2)
def read_bronze_csv(object_name: str) -> pd.DataFrame:
    """
    Lit un fichier CSV depuis le bucket Bronze.
    
    Args:
        object_name: Nom du fichier dans MinIO (ex: "clients.csv")
    
    Returns:
        DataFrame pandas
    """
    client = get_minio_client()

    try:
        response = client.get_object(BUCKET_BRONZE, object_name)
        raw = response.read()
        response.close()
        response.release_conn()

        if not raw:
            print(f"⚠️ Fichier vide dans bronze: {object_name}")
            return pd.DataFrame()

        df = pd.read_csv(BytesIO(raw))

        if df.empty:
            print(f"⚠️ DataFrame vide après lecture de {object_name}")

        print(f"✅ Lu {object_name}: {len(df)} lignes, {len(df.columns)} colonnes")
        return df

    except S3Error as e:
        print(f"❌ Erreur lecture {object_name}: {e}")
        return pd.DataFrame()


@task(name="write_silver_parquet", retries=2)
def write_parquet_to_silver(df: pd.DataFrame, object_name: str) -> str:
    """
    Écrit un DataFrame en Parquet vers le bucket Silver.
    
    Args:
        df: DataFrame à écrire
        object_name: Nom du fichier de sortie (ex: "clients.parquet")
    
    Returns:
        Nom du fichier écrit
    """
    client = get_minio_client()

    if df is None or df.empty:
        print(f"⚠️ DataFrame vide, aucun fichier écrit pour {object_name}")
        return ""

    # Créer le bucket s'il n'existe pas
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)

    # Convertir DataFrame en Parquet (en mémoire)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    # Upload vers MinIO
    client.put_object(
        BUCKET_SILVER,
        object_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    file_size_mb = parquet_buffer.getbuffer().nbytes / (1024 * 1024)
    print(f"✅ Écrit {object_name} vers {BUCKET_SILVER} ({file_size_mb:.2f} MB)")

    return object_name


@task(name="validate_clients_quality")
def validate_clients_quality(df: pd.DataFrame) -> dict:
    """Valide la qualité des données clients"""
    return validate_data_quality(df, QUALITY_RULES["clients"], "clients")


@task(name="validate_achats_quality")
def validate_achats_quality(df: pd.DataFrame) -> dict:
    """Valide la qualité des données achats"""
    return validate_data_quality(df, QUALITY_RULES["achats"], "achats")


@task(name="discover_bronze_files")
def discover_bronze_files() -> list:
    """
    Découvre automatiquement tous les fichiers CSV dans le bucket Bronze.
    
    Returns:
        Liste des noms de fichiers CSV trouvés
    """
    client = get_minio_client()
    
    try:
        objects = client.list_objects(BUCKET_BRONZE, recursive=True)
        csv_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
        
        print(f"📁 Fichiers CSV découverts dans Bronze: {len(csv_files)}")
        for f in csv_files:
            print(f"   - {f}")
        
        return csv_files
    except Exception as e:
        print(f"❌ Erreur lors de la découverte des fichiers: {e}")
        return []


@task(name="validate_generic_quality")
def validate_generic_quality(df: pd.DataFrame, dataset_name: str) -> dict:
    """
    Valide la qualité générique d'un dataset (sans règles spécifiques).
    
    Args:
        df: DataFrame à valider
        dataset_name: Nom du dataset
    
    Returns:
        Dict avec résultats de validation générique
    """
    if df.empty:
        return {"dataset": dataset_name, "total_rows": 0, "checks": {}}
    
    report = {
        "dataset": dataset_name,
        "total_rows": len(df),
        "checks": {}
    }
    
    # Complétude globale
    total_cells = len(df) * len(df.columns)
    null_cells = df.isna().sum().sum()
    completeness = (1 - null_cells / total_cells) * 100 if total_cells > 0 else 0
    
    report["checks"]["global_completeness"] = {
        "value": completeness,
        "status": "✅" if completeness >= 80 else "⚠️"
    }
    
    # Détection de doublons
    duplicates = df.duplicated().sum()
    uniqueness = (1 - duplicates / len(df)) * 100 if len(df) > 0 else 0
    
    report["checks"]["uniqueness"] = {
        "value": uniqueness,
        "status": "✅" if uniqueness >= 95 else "⚠️"
    }
    
    return report


@flow(name="Silver Transformation Flow")
def silver_transformation_flow(use_specific_cleaners: bool = True) -> Dict[str, Any]:
    """
    Flow principal de transformation Silver - VERSION GÉNÉRIQUE.
    
    Processus:
    1. Découvre automatiquement tous les fichiers CSV dans Bronze
    2. Pour chaque fichier:
       - Si use_specific_cleaners=True: utilise les cleaners spécifiques (clients/achats)
       - Sinon: utilise le nettoyage générique intelligent
    3. Valide la qualité
    4. Écrit en Parquet vers Silver
    
    Args:
        use_specific_cleaners: Si True, utilise clean_clients_data/clean_achats_data pour les fichiers connus.
                              Si False, utilise clean_data_generic pour tous les fichiers.
    
    Returns:
        Dict avec les résultats pour chaque fichier traité
    """
    print("🔄 Démarrage transformation Silver (mode générique)...")

    # Découvrir tous les fichiers CSV dans Bronze
    csv_files = discover_bronze_files()
    
    if not csv_files:
        print("⚠️ Aucun fichier CSV trouvé dans Bronze")
        return {"error": "Aucun fichier CSV trouvé dans Bronze"}

    summary: Dict[str, Any] = {}
    quality_reports = []

    # Traiter chaque fichier découvert
    for csv_file in csv_files:
        dataset_name = csv_file.replace('.csv', '')
        print(f"\n📋 Traitement de {csv_file}...")
        
        # Lire depuis Bronze
        df_raw = read_bronze_csv(csv_file)
        
        if df_raw is None or df_raw.empty:
            print(f"⚠️ Fichier {csv_file} vide ou non lisible, ignoré.")
            summary[dataset_name] = {
                "status": "skipped",
                "rows": 0,
                "file": "",
                "quality": {"dataset": dataset_name, "total_rows": 0, "checks": {}}
            }
            continue

        # Nettoyage : utiliser les cleaners spécifiques si disponibles, sinon générique
        if use_specific_cleaners:
            if dataset_name == "clients":
                df_clean = clean_clients_data(df_raw)
                quality = validate_clients_quality(df_clean)
            elif dataset_name == "achats":
                # Pour achats, on a besoin des clients pour l'intégrité référentielle
                # On vérifie si clients a déjà été traité dans ce flow
                valid_client_ids = None
                if "clients" in summary and summary["clients"]["status"] == "ok":
                    # Charger les clients depuis Silver
                    try:
                        client = get_minio_client()
                        response = client.get_object(BUCKET_SILVER, "clients.parquet")
                        import pyarrow.parquet as pq
                        clients_df = pd.read_parquet(BytesIO(response.read()))
                        response.close()
                        response.release_conn()
                        if not clients_df.empty and "id_client" in clients_df.columns:
                            valid_client_ids = clients_df["id_client"]
                    except:
                        pass
                df_clean = clean_achats_data(df_raw, valid_client_ids=valid_client_ids)
                quality = validate_achats_quality(df_clean)
            else:
                # Fichier inconnu : nettoyage générique
                df_clean = clean_data_generic(df_raw, dataset_name)
                quality = validate_generic_quality(df_clean, dataset_name)
        else:
            # Mode entièrement générique
            df_clean = clean_data_generic(df_raw, dataset_name)
            quality = validate_generic_quality(df_clean, dataset_name)

        # Écrire vers Silver
        parquet_name = f"{dataset_name}.parquet"
        parquet_file = write_parquet_to_silver(df_clean, parquet_name)

        summary[dataset_name] = {
            "status": "ok" if not df_clean.empty else "empty",
            "rows": len(df_clean),
            "file": parquet_file,
            "quality": quality,
        }
        
        quality_reports.append(quality)

    # ===== RAPPORT FINAL =====
    if quality_reports:
        generate_quality_report(quality_reports)

    print(f"\n✅ Transformation Silver terminée: {len(summary)} fichier(s) traité(s)")
    return summary


if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver transformation complete: {result}")

