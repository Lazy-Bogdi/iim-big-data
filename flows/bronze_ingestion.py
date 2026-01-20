from io import BytesIO
from pathlib import Path
from typing import Dict, List

from prefect import flow, task
from minio.error import S3Error

from config import BUCKET_BRONZE, BUCKET_SOURCES, get_minio_client


@task(name="upload_to_sources", retries=2)
def upload_csv_to_souces(file_path: str, object_name: str) -> str:
    """
    Upload d'un fichier CSV local vers le bucket MinIO sources.

    Args:
        file_path: Chemin local du fichier CSV
        object_name: Nom de l'objet dans MinIO

    Returns:
        Nom de l'objet dans MinIO
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    print(f"✅ Uploaded {object_name} to {BUCKET_SOURCES}")
    return object_name


@task(name="copy_to_bronze", retries=2)
def copy_to_bronze_layer(object_name: str) -> str:
    """
    Copie un objet du bucket sources vers le bucket bronze (raw layer).

    Args:
        object_name: Nom de l'objet à copier

    Returns:
        Nom de l'objet dans la couche bronze
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)

    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    client.put_object(
        BUCKET_BRONZE,
        object_name,
        BytesIO(data),
        length=len(data),
    )
    print(f"✅ Copied {object_name} to {BUCKET_BRONZE}")
    return object_name


@task(name="discover_csv_files")
def discover_csv_files(data_dir: str) -> List[Path]:
    """
    Découvre tous les fichiers CSV dans un répertoire donné.

    Rend le flow Bronze générique : tout nouveau CSV déposé dans data/sources/
    sera automatiquement pris en compte sans changer le code.
    """
    data_path = Path(data_dir)

    if not data_path.exists() or not data_path.is_dir():
        raise FileNotFoundError(f"Le répertoire de données n'existe pas: {data_dir}")

    csv_files = sorted(data_path.glob("*.csv"))
    if not csv_files:
        print(f"⚠️ Aucun fichier CSV trouvé dans {data_dir}")

    print(f"🔍 Fichiers CSV découverts ({len(csv_files)}): {[f.name for f in csv_files]}")
    return csv_files


@flow(name="Bronze Ingestion Flow")
def bronze_ingestion_flow(data_dir: str = "./data/sources") -> Dict[str, Dict[str, str]]:
    """
    Flow principal : upload des fichiers CSV vers sources et copie vers bronze.

    - Découvre automatiquement tous les fichiers *.csv dans data_dir
    - Upload chaque fichier vers le bucket `sources`
    - Copie chaque objet vers le bucket `bronze`
    - Continue même si un fichier échoue (robustesse à de nouvelles données)

    Args:
        data_dir: Répertoire contenant les fichiers CSV sources

    Returns:
        Dictionnaire avec les fichiers ingérés et les erreurs éventuelles.
        Exemple :
        {
            "success": {"clients.csv": "clients.csv", "achats.csv": "achats.csv"},
            "failed": {"autre.csv": "message d'erreur"}
        }
    """
    csv_files = discover_csv_files(data_dir)

    results_success: Dict[str, str] = {}
    results_failed: Dict[str, str] = {}

    for file_path in csv_files:
        object_name = file_path.name  # on garde le même nom dans MinIO

        try:
            uploaded_name = upload_csv_to_souces(str(file_path), object_name)
            bronze_name = copy_to_bronze_layer(uploaded_name)
            results_success[object_name] = bronze_name
        except (FileNotFoundError, S3Error, OSError) as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"❌ Échec pour {object_name}: {msg}")
            results_failed[object_name] = msg
        except Exception as e:
            msg = f"Unexpected error: {e}"
            print(f"❌ Échec inattendu pour {object_name}: {msg}")
            results_failed[object_name] = msg

    print(f"📦 Ingestion Bronze terminée. Succès: {len(results_success)}, Échecs: {len(results_failed)}")

    return {
        "success": results_success,
        "failed": results_failed,
    }


if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print(f"Bronze ingestion complete: {result}")


