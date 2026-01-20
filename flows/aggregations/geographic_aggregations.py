"""Agrégations géographiques pour la couche Gold"""

import pandas as pd


def aggregate_by_country(fact_achats: pd.DataFrame) -> pd.DataFrame:
    """Agrège les données par pays"""
    agg = fact_achats.groupby('pays').agg({
        'montant': ['sum', 'mean', 'min', 'max', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count',
        'produit': 'nunique'
    }).reset_index()
    
    agg.columns = [
        'pays',
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
    agg['ca_par_client'] = agg['ca_total'] / agg['nb_clients_uniques']
    agg['nb_achats_par_client'] = agg['nb_achats_total'] / agg['nb_clients_uniques']
    
    # Pourcentage du CA total
    total_ca = agg['ca_total'].sum()
    agg['pct_ca_total'] = (agg['ca_total'] / total_ca * 100).round(2)
    
    # Trier par CA décroissant
    agg = agg.sort_values('ca_total', ascending=False)
    
    return agg

