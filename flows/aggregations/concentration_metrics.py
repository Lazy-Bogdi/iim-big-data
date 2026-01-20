"""Métriques de concentration (Pareto, Gini) pour la couche Gold"""

import pandas as pd
import numpy as np


def calculate_concentration_metrics(fact_achats: pd.DataFrame) -> dict:
    """
    Calcule les métriques de concentration (Pareto, Gini).
    
    Args:
        fact_achats: Table de faits
    
    Returns:
        Dict avec métriques de concentration
    """
    # Concentration par client (Pareto)
    client_ca = fact_achats.groupby('id_client')['montant'].sum().sort_values(ascending=False).reset_index()
    client_ca.columns = ['id_client', 'ca_total']
    
    total_ca = client_ca['ca_total'].sum()
    client_ca['ca_cumul'] = client_ca['ca_total'].cumsum()
    client_ca['pct_ca_cumul'] = (client_ca['ca_cumul'] / total_ca * 100)
    client_ca['pct_clients_cumul'] = (np.arange(1, len(client_ca) + 1) / len(client_ca) * 100)
    
    # Règle de Pareto (80/20)
    pareto_80 = client_ca[client_ca['pct_ca_cumul'] <= 80]
    pareto_20_pct = len(pareto_80) / len(client_ca) * 100 if len(client_ca) > 0 else 0
    
    # Top 10% et Top 20%
    top_10_pct = int(len(client_ca) * 0.1)
    top_20_pct = int(len(client_ca) * 0.2)
    
    ca_top_10 = client_ca.head(top_10_pct)['ca_total'].sum() if top_10_pct > 0 else 0
    ca_top_20 = client_ca.head(top_20_pct)['ca_total'].sum() if top_20_pct > 0 else 0
    
    pct_ca_top_10 = (ca_top_10 / total_ca * 100) if total_ca > 0 else 0
    pct_ca_top_20 = (ca_top_20 / total_ca * 100) if total_ca > 0 else 0
    
    # Indice de Gini pour concentration
    def calculate_gini(values):
        """Calcule l'indice de Gini"""
        sorted_values = np.sort(values)
        n = len(sorted_values)
        index = np.arange(1, n + 1)
        return (2 * np.sum(index * sorted_values)) / (n * np.sum(sorted_values)) - (n + 1) / n
    
    gini_clients = calculate_gini(client_ca['ca_total'].values)
    
    # Concentration par pays
    country_ca = fact_achats.groupby('pays')['montant'].sum().sort_values(ascending=False).reset_index()
    country_ca.columns = ['pays', 'ca_total']
    country_ca['pct_ca'] = (country_ca['ca_total'] / total_ca * 100)
    country_ca['ca_cumul'] = country_ca['ca_total'].cumsum()
    country_ca['pct_ca_cumul'] = (country_ca['ca_cumul'] / total_ca * 100)
    
    gini_pays = calculate_gini(country_ca['ca_total'].values)
    
    # Concentration par produit
    product_ca = fact_achats.groupby('produit')['montant'].sum().sort_values(ascending=False).reset_index()
    product_ca.columns = ['produit', 'ca_total']
    product_ca['pct_ca'] = (product_ca['ca_total'] / total_ca * 100)
    product_ca['ca_cumul'] = product_ca['ca_total'].cumsum()
    product_ca['pct_ca_cumul'] = (product_ca['ca_cumul'] / total_ca * 100)
    
    gini_produits = calculate_gini(product_ca['ca_total'].values)
    
    # Résumé
    concentration_summary = pd.DataFrame([{
        'indice_gini_clients': gini_clients,
        'indice_gini_pays': gini_pays,
        'indice_gini_produits': gini_produits,
        'pareto_20_pct_clients': pareto_20_pct,
        'pct_ca_top_10_clients': pct_ca_top_10,
        'pct_ca_top_20_clients': pct_ca_top_20,
        'nb_clients_total': len(client_ca),
        'nb_pays_total': len(country_ca),
        'nb_produits_total': len(product_ca)
    }])
    
    return {
        'client_concentration': client_ca,
        'country_concentration': country_ca,
        'product_concentration': product_ca,
        'concentration_summary': concentration_summary
    }

