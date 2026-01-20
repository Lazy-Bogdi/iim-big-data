"""Flow d'agrégation Gold : calcul des KPIs et métriques métier"""

from io import BytesIO
import pandas as pd
from prefect import flow, task
from minio.error import S3Error

from config import BUCKET_SILVER, BUCKET_GOLD, get_minio_client
from aggregations import (
    create_dim_clients,
    create_dim_produits,
    create_dim_temps,
    create_fact_achats,
    aggregate_by_day,
    aggregate_by_week,
    aggregate_by_month,
    aggregate_by_hour,
    aggregate_by_country,
    calculate_rfm_segmentation,
    calculate_clv_metrics,
    calculate_retention_metrics,
    calculate_product_metrics,
    calculate_cohort_analysis,
    calculate_seasonality,
    calculate_statistical_distributions,
    calculate_concentration_metrics,
    calculate_global_kpis,
    calculate_growth_metrics,
)
from ml import enrich_with_ml


@task(name="read_silver_parquet", retries=2)
def read_silver_parquet(object_name: str) -> pd.DataFrame:
    """
    Lit un fichier Parquet depuis le bucket Silver.
    
    Args:
        object_name: Nom du fichier dans MinIO (ex: "clients.parquet")
    
    Returns:
        DataFrame pandas
    """
    client = get_minio_client()
    
    try:
        response = client.get_object(BUCKET_SILVER, object_name)
        df = pd.read_parquet(BytesIO(response.read()))
        response.close()
        response.release_conn()
        
        print(f"✅ Lu {object_name}: {len(df)} lignes, {len(df.columns)} colonnes")
        return df
    
    except S3Error as e:
        print(f"❌ Erreur lecture {object_name}: {e}")
        raise


@task(name="write_gold_parquet", retries=2)
def write_parquet_to_gold(df: pd.DataFrame, object_path: str) -> str:
    """
    Écrit un DataFrame en Parquet vers le bucket Gold.
    
    Args:
        df: DataFrame à écrire
        object_path: Chemin du fichier dans Gold (ex: "facts/fact_achats.parquet")
    
    Returns:
        Chemin du fichier écrit
    """
    client = get_minio_client()
    
    # Créer le bucket s'il n'existe pas
    if not client.bucket_exists(BUCKET_GOLD):
        client.make_bucket(BUCKET_GOLD)
    
    # Convertir DataFrame en Parquet (en mémoire)
    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
    parquet_buffer.seek(0)
    
    # Upload vers MinIO
    client.put_object(
        BUCKET_GOLD,
        object_path,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )
    
    file_size_mb = parquet_buffer.getbuffer().nbytes / (1024 * 1024)
    print(f"✅ Écrit {object_path} vers {BUCKET_GOLD} ({file_size_mb:.2f} MB)")
    
    return object_path


@flow(name="Gold Aggregation Flow")
def gold_aggregation_flow() -> dict:
    """
    Flow principal d'agrégation Gold.
    
    Processus complet:
    1. Lire données depuis Silver
    2. Créer dimensions
    3. Créer table de faits
    4. Calculer toutes les agrégations et métriques
    5. Écrire tout en Parquet vers Gold
    
    Returns:
        Dict avec tous les fichiers créés
    """
    print("🔄 Démarrage agrégation Gold...")
    
    # ===== LECTURE DEPUIS SILVER =====
    print("\n📖 Lecture des données Silver...")
    clients_df = read_silver_parquet("clients.parquet")
    achats_df = read_silver_parquet("achats.parquet")
    
    # ===== ENRICHISSEMENT ML =====
    print("\n🤖 Enrichissement avec Machine Learning...")
    if not clients_df.empty:
        clients_df = enrich_with_ml(clients_df, "clients")
        # Sauvegarder la version enrichie ML
        write_parquet_to_gold(clients_df, "ml/clients_enriched_ml.parquet")
    
    if not achats_df.empty:
        achats_df = enrich_with_ml(achats_df, "achats")
        # Sauvegarder la version enrichie ML
        write_parquet_to_gold(achats_df, "ml/achats_enriched_ml.parquet")
    
    # ===== DIMENSIONS =====
    print("\n📐 Création des dimensions...")
    dim_clients = create_dim_clients(clients_df)
    dim_produits = create_dim_produits(achats_df)
    
    # Dimension temps (calendrier)
    date_min = pd.to_datetime(achats_df['date_achat']).min()
    date_max = pd.to_datetime(achats_df['date_achat']).max()
    dim_temps = create_dim_temps(date_min, date_max)
    
    # Écrire dimensions
    write_parquet_to_gold(dim_clients, "dimensions/dim_clients.parquet")
    write_parquet_to_gold(dim_produits, "dimensions/dim_produits.parquet")
    write_parquet_to_gold(dim_temps, "dimensions/dim_temps.parquet")
    
    # ===== TABLE DE FAITS =====
    print("\n📊 Création de la table de faits...")
    fact_achats = create_fact_achats(achats_df, clients_df)
    write_parquet_to_gold(fact_achats, "facts/fact_achats.parquet")
    
    # ===== AGRÉGATIONS TEMPORELLES =====
    print("\n⏰ Calcul des agrégations temporelles...")
    fact_ca_jour = aggregate_by_day(fact_achats)
    fact_ca_semaine = aggregate_by_week(fact_achats)
    fact_ca_mois = aggregate_by_month(fact_achats)
    fact_ca_heure = aggregate_by_hour(fact_achats)
    
    write_parquet_to_gold(fact_ca_jour, "facts/fact_ca_jour.parquet")
    write_parquet_to_gold(fact_ca_semaine, "facts/fact_ca_semaine.parquet")
    write_parquet_to_gold(fact_ca_mois, "facts/fact_ca_mois.parquet")
    write_parquet_to_gold(fact_ca_heure, "facts/fact_ca_heure.parquet")
    
    # ===== AGRÉGATIONS GÉOGRAPHIQUES =====
    print("\n🌍 Calcul des agrégations géographiques...")
    fact_ca_pays = aggregate_by_country(fact_achats)
    write_parquet_to_gold(fact_ca_pays, "facts/fact_ca_pays.parquet")
    
    # ===== SEGMENTATION RFM =====
    print("\n🎯 Calcul de la segmentation RFM...")
    rfm_detail, rfm_summary = calculate_rfm_segmentation(fact_achats)
    write_parquet_to_gold(rfm_detail, "dimensions/dim_rfm.parquet")
    write_parquet_to_gold(rfm_summary, "kpis/kpi_rfm.parquet")
    
    # ===== CLV METRICS =====
    print("\n💰 Calcul des métriques CLV...")
    clv_detail, clv_by_country = calculate_clv_metrics(fact_achats, clients_df)
    write_parquet_to_gold(clv_detail, "kpis/kpi_clv_detail.parquet")
    write_parquet_to_gold(clv_by_country, "kpis/kpi_clv_pays.parquet")
    
    # ===== RETENTION METRICS =====
    print("\n🔄 Calcul des métriques de rétention...")
    retention_results = calculate_retention_metrics(fact_achats, clients_df)
    write_parquet_to_gold(retention_results['retention_detail'], "kpis/kpi_retention_detail.parquet")
    write_parquet_to_gold(retention_results['retention_summary'], "kpis/kpi_retention_summary.parquet")
    write_parquet_to_gold(retention_results['global_metrics'], "kpis/kpi_retention_global.parquet")
    
    # ===== PRODUCT METRICS =====
    print("\n📦 Calcul des métriques produits...")
    product_results = calculate_product_metrics(fact_achats)
    write_parquet_to_gold(product_results['product_metrics'], "kpis/kpi_produits.parquet")
    write_parquet_to_gold(product_results['top_products_ca'], "kpis/kpi_top_produits_ca.parquet")
    write_parquet_to_gold(product_results['top_products_volume'], "kpis/kpi_top_produits_volume.parquet")
    write_parquet_to_gold(product_results['basket_diversity'], "kpis/kpi_diversite_panier.parquet")
    write_parquet_to_gold(product_results['diversity_summary'], "kpis/kpi_diversite_summary.parquet")
    
    # ===== COHORT ANALYSIS =====
    print("\n👥 Calcul de l'analyse par cohortes...")
    cohort_results = calculate_cohort_analysis(fact_achats, clients_df)
    write_parquet_to_gold(cohort_results['cohort_ca'], "analytics/analytics_cohortes_ca.parquet")
    write_parquet_to_gold(cohort_results['cohort_total'], "analytics/analytics_cohortes_total.parquet")
    write_parquet_to_gold(cohort_results['cohort_retention'], "analytics/analytics_cohortes_retention.parquet")
    
    # ===== SEASONALITY =====
    print("\n📅 Calcul de l'analyse de saisonnalité...")
    seasonality_results = calculate_seasonality(fact_achats)
    write_parquet_to_gold(seasonality_results['by_day_of_week'], "analytics/analytics_saisonnalite_jour.parquet")
    write_parquet_to_gold(seasonality_results['by_hour'], "analytics/analytics_saisonnalite_heure.parquet")
    write_parquet_to_gold(seasonality_results['by_month'], "analytics/analytics_saisonnalite_mois.parquet")
    write_parquet_to_gold(seasonality_results['weekend_vs_week'], "analytics/analytics_saisonnalite_weekend.parquet")
    
    # ===== STATISTICAL DISTRIBUTIONS =====
    print("\n📈 Calcul des distributions statistiques...")
    stats_results = calculate_statistical_distributions(fact_achats)
    write_parquet_to_gold(stats_results['distributions_summary'], "analytics/analytics_distributions_summary.parquet")
    write_parquet_to_gold(stats_results['client_stats'], "analytics/analytics_distributions_clients.parquet")
    write_parquet_to_gold(stats_results['produit_stats'], "analytics/analytics_distributions_produits.parquet")
    
    # ===== CONCENTRATION METRICS =====
    print("\n📊 Calcul des métriques de concentration...")
    concentration_results = calculate_concentration_metrics(fact_achats)
    write_parquet_to_gold(concentration_results['concentration_summary'], "analytics/analytics_concentration_summary.parquet")
    write_parquet_to_gold(concentration_results['client_concentration'], "analytics/analytics_concentration_clients.parquet")
    write_parquet_to_gold(concentration_results['country_concentration'], "analytics/analytics_concentration_pays.parquet")
    write_parquet_to_gold(concentration_results['product_concentration'], "analytics/analytics_concentration_produits.parquet")
    
    # ===== GLOBAL KPIS =====
    print("\n🎯 Calcul des KPIs globaux...")
    global_kpis = calculate_global_kpis(fact_achats, clients_df)
    write_parquet_to_gold(global_kpis, "kpis/kpi_globaux.parquet")
    
    # ===== GROWTH METRICS =====
    print("\n📈 Calcul des métriques de croissance...")
    growth_results = calculate_growth_metrics(fact_achats)
    write_parquet_to_gold(growth_results['monthly_growth'], "kpis/kpi_croissance_mensuelle.parquet")
    write_parquet_to_gold(growth_results['growth_summary'], "kpis/kpi_croissance_summary.parquet")
    
    # ===== RÉSUMÉ FINAL =====
    result = {
        "dimensions": 3,
        "facts": 5,
        "kpis": 10,
        "analytics": 8,
        "total_files": 26
    }
    
    print("\n" + "="*60)
    print("✅ AGRÉGATION GOLD TERMINÉE")
    print("="*60)
    print(f"📐 Dimensions créées: {result['dimensions']}")
    print(f"📊 Tables de faits créées: {result['facts']}")
    print(f"🎯 KPIs calculés: {result['kpis']}")
    print(f"📈 Analyses créées: {result['analytics']}")
    print(f"📁 Total fichiers Parquet: {result['total_files']}")
    print("="*60 + "\n")
    
    return result


if __name__ == "__main__":
    result = gold_aggregation_flow()
    print(f"Gold aggregation complete: {result}")

