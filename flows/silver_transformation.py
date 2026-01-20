"""Flow de transformation Silver : nettoyage et normalisation des données"""

from io import BytesIO
import pandas as pd
from prefect import flow, task
from minio.error import S3Error

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client
from transformations.data_cleaning import clean_clients_data, clean_achats_data
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
        df = pd.read_csv(BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        print(f"✅ Lu {object_name}: {len(df)} lignes, {len(df.columns)} colonnes")
        return df
    
    except S3Error as e:
        print(f"❌ Erreur lecture {object_name}: {e}")
        raise


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
    
    # Créer le bucket s'il n'existe pas
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Convertir DataFrame en Parquet (en mémoire)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    # Upload vers MinIO
    client.put_object(
        BUCKET_SILVER,
        object_name,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
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


@flow(name="Silver Transformation Flow")
def silver_transformation_flow() -> dict:
    """
    Flow principal de transformation Silver.
    
    Processus:
    1. Lire clients.csv depuis Bronze
    2. Nettoyer clients
    3. Valider qualité clients
    4. Écrire clients.parquet vers Silver
    5. Lire achats.csv depuis Bronze
    6. Nettoyer achats (avec validation référentielle)
    7. Valider qualité achats
    8. Écrire achats.parquet vers Silver
    
    Returns:
        Dict avec les noms des fichiers créés
    """
    print("🔄 Démarrage transformation Silver...")
    
    # ===== TRAITEMENT CLIENTS =====
    print("\n📋 Traitement des clients...")
    
    # 1. Lire depuis Bronze
    clients_raw = read_bronze_csv("clients.csv")
    
    # 2. Nettoyer
    clients_clean = clean_clients_data(clients_raw)
    
    # 3. Valider qualité
    clients_quality = validate_clients_quality(clients_clean)
    
    # 4. Écrire vers Silver
    clients_parquet = write_parquet_to_silver(clients_clean, "clients.parquet")
    
    # ===== TRAITEMENT ACHATS =====
    print("\n📋 Traitement des achats...")
    
    # 5. Lire depuis Bronze
    achats_raw = read_bronze_csv("achats.csv")
    
    # 6. Nettoyer (avec validation référentielle)
    valid_client_ids = clients_clean["id_client"]
    achats_clean = clean_achats_data(achats_raw, valid_client_ids=valid_client_ids)
    
    # 7. Valider qualité
    achats_quality = validate_achats_quality(achats_clean)
    
    # 8. Écrire vers Silver
    achats_parquet = write_parquet_to_silver(achats_clean, "achats.parquet")
    
    # ===== RAPPORT FINAL =====
    generate_quality_report([clients_quality, achats_quality])
    
    result = {
        "clients": clients_parquet,
        "achats": achats_parquet,
        "clients_rows": len(clients_clean),
        "achats_rows": len(achats_clean)
    }
    
    print(f"✅ Transformation Silver terminée: {result}")
    return result


if __name__ == "__main__":
    result = silver_transformation_flow()
    print(f"Silver transformation complete: {result}")

