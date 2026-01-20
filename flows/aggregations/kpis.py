"""KPIs globaux et métriques de croissance pour la couche Gold"""

import pandas as pd
import numpy as np


def calculate_global_kpis(fact_achats: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les KPIs globaux.
    
    Args:
        fact_achats: Table de faits
        clients_df: DataFrame clients
    
    Returns:
        DataFrame avec KPIs globaux
    """
    # Métriques de base
    total_ca = fact_achats['montant'].sum()
    nb_achats = len(fact_achats)
    nb_clients = fact_achats['id_client'].nunique()
    nb_produits = fact_achats['produit'].nunique()
    nb_pays = clients_df['pays'].nunique()
    
    # Métriques calculées
    panier_moyen = total_ca / nb_achats if nb_achats > 0 else 0
    ca_par_client = total_ca / nb_clients if nb_clients > 0 else 0
    nb_achats_par_client = nb_achats / nb_clients if nb_clients > 0 else 0
    
    # Période
    date_min = fact_achats['date_achat'].min()
    date_max = fact_achats['date_achat'].max()
    duree_jours = (pd.to_datetime(date_max) - pd.to_datetime(date_min)).days
    
    # Métriques temporelles
    ca_par_jour = total_ca / duree_jours if duree_jours > 0 else 0
    nb_achats_par_jour = nb_achats / duree_jours if duree_jours > 0 else 0
    
    # Clients actifs
    now = pd.Timestamp.now()
    clients_actifs_30j = fact_achats[
        pd.to_datetime(fact_achats['date_achat']) >= (now - pd.Timedelta(days=30))
    ]['id_client'].nunique()
    
    clients_actifs_90j = fact_achats[
        pd.to_datetime(fact_achats['date_achat']) >= (now - pd.Timedelta(days=90))
    ]['id_client'].nunique()
    
    # Nouveaux clients
    nouveaux_clients = len(clients_df[
        pd.to_datetime(clients_df['date_inscription']) >= (now - pd.Timedelta(days=30))
    ])
    
    kpis = pd.DataFrame([{
        'total_ca': total_ca,
        'nb_achats': nb_achats,
        'nb_clients': nb_clients,
        'nb_produits': nb_produits,
        'nb_pays': nb_pays,
        'panier_moyen': panier_moyen,
        'ca_par_client': ca_par_client,
        'nb_achats_par_client': nb_achats_par_client,
        'ca_par_jour': ca_par_jour,
        'nb_achats_par_jour': nb_achats_par_jour,
        'clients_actifs_30j': clients_actifs_30j,
        'clients_actifs_90j': clients_actifs_90j,
        'nouveaux_clients_30j': nouveaux_clients,
        'date_debut': date_min,
        'date_fin': date_max,
        'duree_jours': duree_jours
    }])
    
    return kpis


def calculate_growth_metrics(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule les métriques de croissance.
    
    Args:
        fact_achats: Table de faits
    
    Returns:
        DataFrame avec métriques de croissance
    """
    # Agrégation par mois
    fact_achats['annee_mois'] = pd.to_datetime(fact_achats['date_achat']).dt.to_period('M').astype(str)
    
    monthly = fact_achats.groupby('annee_mois').agg({
        'montant': 'sum',
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    monthly.columns = ['annee_mois', 'ca_total', 'nb_clients', 'nb_achats']
    monthly = monthly.sort_values('annee_mois')
    
    # Croissance MoM (Month over Month)
    monthly['ca_prev'] = monthly['ca_total'].shift(1)
    monthly['taux_croissance_mom'] = (
        (monthly['ca_total'] - monthly['ca_prev']) / monthly['ca_prev'] * 100
    ).fillna(0)
    
    # Croissance YoY (Year over Year)
    monthly['annee'] = pd.to_datetime(monthly['annee_mois']).dt.year
    monthly['mois'] = pd.to_datetime(monthly['annee_mois']).dt.month
    
    # Créer une copie pour la comparaison YoY
    monthly_yoy = monthly.copy()
    monthly_yoy['annee_prev'] = monthly_yoy['annee'] - 1
    
    # Joindre avec les données de l'année précédente
    monthly_prev = monthly[['annee', 'mois', 'ca_total']].copy()
    monthly_prev.columns = ['annee_prev', 'mois', 'ca_yoy']
    
    monthly_yoy = monthly_yoy.merge(
        monthly_prev,
        on=['annee_prev', 'mois'],
        how='left'
    )
    
    monthly_yoy['taux_croissance_yoy'] = (
        (monthly_yoy['ca_total'] - monthly_yoy['ca_yoy']) / monthly_yoy['ca_yoy'] * 100
    ).fillna(0)
    
    # Sélectionner colonnes pertinentes
    growth_metrics = monthly_yoy[[
        'annee_mois', 'ca_total', 'nb_clients', 'nb_achats',
        'taux_croissance_mom', 'taux_croissance_yoy'
    ]].copy()
    
    # Croissance annualisée
    premier_mois = 0
    dernier_mois = 0
    if len(growth_metrics) > 1:
        premier_mois = growth_metrics.iloc[0]['ca_total']
        dernier_mois = growth_metrics.iloc[-1]['ca_total']
        nb_mois = len(growth_metrics)
        
        if premier_mois > 0 and nb_mois > 0:
            taux_croissance_total = ((dernier_mois / premier_mois) ** (12 / nb_mois) - 1) * 100
        else:
            taux_croissance_total = 0
    else:
        taux_croissance_total = 0
        if len(growth_metrics) > 0:
            premier_mois = growth_metrics.iloc[0]['ca_total']
            dernier_mois = growth_metrics.iloc[0]['ca_total']
    
    # Ajouter métriques globales
    growth_summary = pd.DataFrame([{
        'taux_croissance_annualise': taux_croissance_total,
        'ca_premier_mois': premier_mois,
        'ca_dernier_mois': dernier_mois,
        'nb_mois': len(growth_metrics)
    }])
    
    return {
        'monthly_growth': growth_metrics,
        'growth_summary': growth_summary
    }

