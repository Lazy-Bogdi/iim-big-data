"""Métriques de performance produits pour la couche Gold"""

import pandas as pd


def calculate_product_metrics(fact_achats: pd.DataFrame) -> dict:
    """
    Calcule les métriques de performance par produit.
    
    Args:
        fact_achats: Table de faits
    
    Returns:
        Dict avec différentes métriques produits
    """
    # Métriques par produit
    product_metrics = fact_achats.groupby('produit').agg({
        'montant': ['sum', 'mean', 'min', 'max', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count',
        'date_achat': ['min', 'max']
    }).reset_index()
    
    product_metrics.columns = [
        'produit',
        'ca_total',
        'ca_moyen',
        'ca_min',
        'ca_max',
        'nb_achats',
        'nb_clients_uniques',
        'nb_achats_total',
        'premier_achat',
        'dernier_achat'
    ]
    
    product_metrics['panier_moyen'] = product_metrics['ca_total'] / product_metrics['nb_achats_total']
    product_metrics['ca_par_client'] = product_metrics['ca_total'] / product_metrics['nb_clients_uniques']
    
    # Pourcentage du CA total
    total_ca = product_metrics['ca_total'].sum()
    product_metrics['pct_ca_total'] = (product_metrics['ca_total'] / total_ca * 100).round(2)
    
    # Top produits
    top_products_ca = product_metrics.nlargest(10, 'ca_total')[['produit', 'ca_total', 'nb_achats_total', 'pct_ca_total']]
    top_products_volume = product_metrics.nlargest(10, 'nb_achats_total')[['produit', 'nb_achats_total', 'ca_total']]
    
    # Produits complémentaires (achetés ensemble par même client)
    client_products = fact_achats.groupby('id_client')['produit'].apply(list).reset_index()
    
    # Analyse de diversité du panier
    basket_diversity = fact_achats.groupby('id_client').agg({
        'produit': 'nunique',
        'id_achat': 'count',
        'montant': 'sum'
    }).reset_index()
    
    basket_diversity.columns = ['id_client', 'nb_produits_differents', 'nb_achats_total', 'ca_total']
    basket_diversity['diversite_moyenne'] = basket_diversity['nb_produits_differents'] / basket_diversity['nb_achats_total']
    
    # Clients mono-produit vs multi-produits
    basket_diversity['type_client'] = basket_diversity.apply(
        lambda row: 'Mono-produit' if row['nb_produits_differents'] == 1 else 'Multi-produits',
        axis=1
    )
    
    diversity_summary = basket_diversity.groupby('type_client').agg({
        'id_client': 'count',
        'nb_produits_differents': 'mean',
        'ca_total': 'mean'
    }).reset_index()
    
    return {
        'product_metrics': product_metrics,
        'top_products_ca': top_products_ca,
        'top_products_volume': top_products_volume,
        'basket_diversity': basket_diversity,
        'diversity_summary': diversity_summary
    }

