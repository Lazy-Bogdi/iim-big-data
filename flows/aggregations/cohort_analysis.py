"""Analyse par cohortes d'inscription pour la couche Gold"""

import pandas as pd


def calculate_cohort_analysis(fact_achats: pd.DataFrame, clients_df: pd.DataFrame) -> dict:
    """
    Calcule l'analyse par cohortes d'inscription.
    
    Args:
        fact_achats: Table de faits
        clients_df: DataFrame clients
    
    Returns:
        Dict avec analyses par cohortes
    """
    # Vérifier si date_inscription existe déjà dans fact_achats
    if 'date_inscription' not in fact_achats.columns:
        # Joindre pour avoir date d'inscription si elle n'existe pas
        fact_with_cohort = fact_achats.merge(
            clients_df[['id_client', 'date_inscription']],
            on='id_client',
            how='left'
        )
    else:
        fact_with_cohort = fact_achats.copy()
    
    fact_with_cohort['date_inscription'] = pd.to_datetime(fact_with_cohort['date_inscription'], errors='coerce')
    
    # Filtrer les lignes sans date_inscription valide
    fact_with_cohort = fact_with_cohort.dropna(subset=['date_inscription'])
    
    fact_with_cohort['cohorte_mois'] = fact_with_cohort['date_inscription'].dt.to_period('M').astype(str)
    fact_with_cohort['mois_achat'] = pd.to_datetime(fact_with_cohort['date_achat']).dt.to_period('M').astype(str)
    
    # CA par cohorte et mois
    cohort_ca = fact_with_cohort.groupby(['cohorte_mois', 'mois_achat']).agg({
        'montant': 'sum',
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    cohort_ca.columns = ['cohorte_mois', 'mois_achat', 'ca_total', 'nb_clients', 'nb_achats']
    
    # Calculer l'âge de la cohorte (mois depuis inscription)
    cohort_ca['cohorte_date'] = pd.to_datetime(cohort_ca['cohorte_mois'])
    cohort_ca['achat_date'] = pd.to_datetime(cohort_ca['mois_achat'])
    cohort_ca['age_cohorte_mois'] = (
        (cohort_ca['achat_date'].dt.year - cohort_ca['cohorte_date'].dt.year) * 12 +
        (cohort_ca['achat_date'].dt.month - cohort_ca['cohorte_date'].dt.month)
    ).fillna(0).astype(int)
    
    # CA total par cohorte
    cohort_total = fact_with_cohort.groupby('cohorte_mois').agg({
        'montant': 'sum',
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    cohort_total.columns = ['cohorte_mois', 'ca_total', 'nb_clients', 'nb_achats']
    cohort_total['ca_par_client'] = cohort_total['ca_total'] / cohort_total['nb_clients']
    cohort_total['nb_achats_par_client'] = cohort_total['nb_achats'] / cohort_total['nb_clients']
    
    # Rétention par cohorte (clients actifs chaque mois)
    cohort_retention = fact_with_cohort.groupby(['cohorte_mois', 'mois_achat'])['id_client'].nunique().reset_index()
    cohort_retention.columns = ['cohorte_mois', 'mois_achat', 'nb_clients_actifs']
    
    # Nombre total de clients par cohorte
    cohort_size = fact_with_cohort.groupby('cohorte_mois')['id_client'].nunique().reset_index()
    cohort_size.columns = ['cohorte_mois', 'taille_cohorte']
    
    cohort_retention = cohort_retention.merge(cohort_size, on='cohorte_mois')
    cohort_retention['taux_retention'] = (
        cohort_retention['nb_clients_actifs'] / cohort_retention['taille_cohorte'] * 100
    )
    
    return {
        'cohort_ca': cohort_ca,
        'cohort_total': cohort_total,
        'cohort_retention': cohort_retention
    }

