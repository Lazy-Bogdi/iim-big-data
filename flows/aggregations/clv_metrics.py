"""Métriques Customer Lifetime Value (CLV) pour la couche Gold"""

import pandas as pd
import numpy as np


def calculate_clv_metrics(fact_achats: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le Customer Lifetime Value pour chaque client.
    
    Args:
        fact_achats: Table de faits
        clients_df: DataFrame clients
    
    Returns:
        DataFrame avec métriques CLV
    """
    # Métriques par client
    clv = fact_achats.groupby('id_client').agg({
        'montant': ['sum', 'mean', 'count'],
        'date_achat': ['min', 'max'],
        'produit': 'nunique'
    }).reset_index()
    
    clv.columns = [
        'id_client',
        'clv_total',
        'panier_moyen',
        'nb_achats',
        'premier_achat',
        'dernier_achat',
        'nb_produits_differents'
    ]
    
    # Calculer durée de vie client (en jours)
    clv['premier_achat'] = pd.to_datetime(clv['premier_achat'])
    clv['dernier_achat'] = pd.to_datetime(clv['dernier_achat'])
    clv['duree_vie_jours'] = (clv['dernier_achat'] - clv['premier_achat']).dt.days
    clv['duree_vie_jours'] = clv['duree_vie_jours'].fillna(0)
    
    # Fréquence d'achat (achats par mois)
    clv['frequence_mensuelle'] = clv.apply(
        lambda row: row['nb_achats'] / max(row['duree_vie_jours'] / 30, 1),
        axis=1
    )
    
    # CLV prédictif (projection sur 12 mois)
    clv['clv_predictif_12m'] = clv['frequence_mensuelle'] * clv['panier_moyen'] * 12
    
    # Joindre avec informations clients
    clv = clv.merge(
        clients_df[['id_client', 'pays', 'date_inscription']],
        on='id_client',
        how='left'
    )
    
    # CLV moyen par pays
    clv_by_country = clv.groupby('pays').agg({
        'clv_total': ['mean', 'sum', 'count'],
        'clv_predictif_12m': 'mean',
        'nb_achats': 'mean',
        'panier_moyen': 'mean'
    }).reset_index()
    
    clv_by_country.columns = [
        'pays',
        'clv_moyen',
        'clv_total',
        'nb_clients',
        'clv_predictif_moyen',
        'nb_achats_moyen',
        'panier_moyen'
    ]
    
    return clv, clv_by_country

