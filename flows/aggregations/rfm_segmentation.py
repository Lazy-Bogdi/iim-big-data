"""Segmentation RFM (Recency, Frequency, Monetary) pour la couche Gold"""

import pandas as pd
from datetime import datetime, timedelta


def calculate_rfm_segmentation(fact_achats: pd.DataFrame, reference_date: pd.Timestamp = None) -> pd.DataFrame:
    """
    Calcule la segmentation RFM pour chaque client.
    
    Args:
        fact_achats: Table de faits
        reference_date: Date de référence (par défaut: aujourd'hui)
    
    Returns:
        DataFrame avec scores RFM et segments
    """
    if reference_date is None:
        reference_date = pd.Timestamp.now()
    
    # Calculer métriques RFM par client
    rfm = fact_achats.groupby('id_client').agg({
        'date_achat': 'max',  # Dernier achat
        'id_achat': 'count',  # Fréquence
        'montant': 'sum'      # Montant total
    }).reset_index()
    
    rfm.columns = ['id_client', 'dernier_achat', 'frequency', 'monetary']
    
    # Calculer Recency (jours depuis dernier achat)
    rfm['recency'] = (reference_date - pd.to_datetime(rfm['dernier_achat'])).dt.days
    
    # Créer des scores de 1 à 5 pour chaque dimension
    # Gérer le cas où il n'y a pas assez de valeurs uniques
    try:
        rfm['R_score'] = pd.qcut(rfm['recency'], q=5, labels=[5, 4, 3, 2, 1], duplicates='drop')
    except ValueError:
        rfm['R_score'] = pd.cut(rfm['recency'], bins=5, labels=[5, 4, 3, 2, 1], duplicates='drop')
    
    try:
        rfm['F_score'] = pd.qcut(rfm['frequency'], q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    except ValueError:
        rfm['F_score'] = pd.cut(rfm['frequency'], bins=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    
    try:
        rfm['M_score'] = pd.qcut(rfm['monetary'], q=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    except ValueError:
        rfm['M_score'] = pd.cut(rfm['monetary'], bins=5, labels=[1, 2, 3, 4, 5], duplicates='drop')
    
    # Convertir en int
    rfm['R_score'] = rfm['R_score'].astype(int)
    rfm['F_score'] = rfm['F_score'].astype(int)
    rfm['M_score'] = rfm['M_score'].astype(int)
    
    # Score RFM combiné
    rfm['RFM_score'] = (
        rfm['R_score'].astype(str) + 
        rfm['F_score'].astype(str) + 
        rfm['M_score'].astype(str)
    )
    
    # Segmentation
    def assign_segment(row):
        r, f, m = row['R_score'], row['F_score'], row['M_score']
        
        if r >= 4 and f >= 4 and m >= 4:
            return 'Champions'
        elif r >= 3 and f >= 3 and m >= 3:
            return 'Loyal'
        elif r >= 3 and f <= 2 and m >= 3:
            return 'Potential Loyalist'
        elif r >= 4 and f <= 2 and m <= 2:
            return 'New Customers'
        elif r <= 2 and f >= 3 and m >= 3:
            return 'At Risk'
        elif r <= 2 and f <= 2 and m <= 2:
            return 'Lost'
        elif r <= 2 and f >= 3 and m <= 2:
            return 'Hibernating'
        else:
            return 'Need Attention'
    
    rfm['segment'] = rfm.apply(assign_segment, axis=1)
    
    # Statistiques par segment
    rfm_summary = rfm.groupby('segment').agg({
        'id_client': 'count',
        'recency': 'mean',
        'frequency': 'mean',
        'monetary': ['mean', 'sum']
    }).reset_index()
    
    rfm_summary.columns = [
        'segment',
        'nb_clients',
        'recency_moyen',
        'frequency_moyen',
        'monetary_moyen',
        'monetary_total'
    ]
    
    return rfm, rfm_summary

