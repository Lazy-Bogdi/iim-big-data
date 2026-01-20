"""Distributions statistiques pour la couche Gold"""

import pandas as pd
import numpy as np


def calculate_statistical_distributions(fact_achats: pd.DataFrame) -> dict:
    """
    Calcule les distributions statistiques.
    
    Args:
        fact_achats: Table de faits
    
    Returns:
        Dict avec distributions statistiques
    """
    # Distribution des montants
    montant_stats = fact_achats['montant'].describe()
    
    # Quartiles et déciles
    quartiles = fact_achats['montant'].quantile([0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
    deciles = fact_achats['montant'].quantile([i/10 for i in range(1, 10)])
    
    # Skewness et Kurtosis
    skewness = fact_achats['montant'].skew()
    kurtosis = fact_achats['montant'].kurtosis()
    
    # Distribution par client
    client_stats = fact_achats.groupby('id_client').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_achat': 'count'
    }).reset_index()
    
    client_stats.columns = ['id_client', 'ca_total', 'panier_moyen', 'nb_achats', 'nb_achats_total']
    
    ca_total_stats = client_stats['ca_total'].describe()
    nb_achats_stats = client_stats['nb_achats_total'].describe()
    
    # Distribution par produit
    produit_stats = fact_achats.groupby('produit')['montant'].agg(['mean', 'std', 'min', 'max']).reset_index()
    produit_stats.columns = ['produit', 'montant_moyen', 'montant_std', 'montant_min', 'montant_max']
    
    # Outliers (méthode IQR)
    Q1 = fact_achats['montant'].quantile(0.25)
    Q3 = fact_achats['montant'].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    outliers = fact_achats[
        (fact_achats['montant'] < lower_bound) | 
        (fact_achats['montant'] > upper_bound)
    ]
    
    # Résumé des distributions
    distributions_summary = pd.DataFrame([{
        'metrique': 'montant',
        'mean': montant_stats['mean'],
        'median': montant_stats['50%'],
        'std': montant_stats['std'],
        'min': montant_stats['min'],
        'max': montant_stats['max'],
        'q25': quartiles[0.25],
        'q75': quartiles[0.75],
        'q90': quartiles[0.90],
        'q95': quartiles[0.95],
        'q99': quartiles[0.99],
        'skewness': skewness,
        'kurtosis': kurtosis,
        'nb_outliers': len(outliers),
        'pct_outliers': len(outliers) / len(fact_achats) * 100
    }])
    
    return {
        'montant_stats': montant_stats,
        'quartiles': quartiles,
        'deciles': deciles,
        'distributions_summary': distributions_summary,
        'client_stats': client_stats,
        'produit_stats': produit_stats,
        'outliers': outliers
    }

