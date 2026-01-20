"""Agrégations temporelles pour la couche Gold"""

import pandas as pd
from typing import Dict


def aggregate_by_day(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par jour"""
    agg = fact_achats.groupby('date_achat').agg({
        'montant': ['sum', 'mean', 'min', 'max', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count',
        'produit': 'nunique'
    }).reset_index()
    
    agg.columns = [
        'date',
        'ca_total',
        'ca_moyen',
        'ca_min',
        'ca_max',
        'nb_achats',
        'nb_clients_uniques',
        'nb_achats_total',
        'nb_produits_differents'
    ]
    
    agg['panier_moyen'] = agg['ca_total'] / agg['nb_achats']
    
    return agg


def aggregate_by_week(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par semaine"""
    agg = fact_achats.groupby('annee_semaine').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count',
        'produit': 'nunique',
        'date_achat': ['min', 'max']
    }).reset_index()
    
    agg.columns = [
        'annee_semaine',
        'ca_total',
        'ca_moyen',
        'nb_achats',
        'nb_clients_uniques',
        'nb_achats_total',
        'nb_produits_differents',
        'date_debut',
        'date_fin'
    ]
    
    agg['panier_moyen'] = agg['ca_total'] / agg['nb_achats']
    
    return agg


def aggregate_by_month(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par mois"""
    agg = fact_achats.groupby('annee_mois').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count',
        'produit': 'nunique',
        'date_achat': ['min', 'max']
    }).reset_index()
    
    agg.columns = [
        'annee_mois',
        'ca_total',
        'ca_moyen',
        'nb_achats',
        'nb_clients_uniques',
        'nb_achats_total',
        'nb_produits_differents',
        'date_debut',
        'date_fin'
    ]
    
    agg['panier_moyen'] = agg['ca_total'] / agg['nb_achats']
    
    # Calculer taux de croissance MoM
    agg = agg.sort_values('annee_mois')
    agg['ca_prev'] = agg['ca_total'].shift(1)
    agg['taux_croissance_mom'] = ((agg['ca_total'] - agg['ca_prev']) / agg['ca_prev'] * 100).fillna(0)
    
    return agg


def aggregate_by_hour(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par heure de la journée"""
    agg = fact_achats.groupby('heure').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    agg.columns = [
        'heure',
        'ca_total',
        'ca_moyen',
        'nb_achats',
        'nb_clients_uniques',
        'nb_achats_total'
    ]
    
    agg['panier_moyen'] = agg['ca_total'] / agg['nb_achats']
    
    return agg

