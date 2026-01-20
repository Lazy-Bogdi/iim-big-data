"""Métriques de rétention et churn pour la couche Gold"""

import pandas as pd
from datetime import timedelta


def calculate_retention_metrics(fact_achats: pd.DataFrame, clients_df: pd.DataFrame, reference_date: pd.Timestamp = None) -> dict:
    """
    Calcule les métriques de rétention et churn.
    
    Args:
        fact_achats: Table de faits
        clients_df: DataFrame clients
        reference_date: Date de référence
    
    Returns:
        Dict avec différentes métriques de rétention
    """
    if reference_date is None:
        reference_date = pd.Timestamp.now()
    
    # Dernier achat par client
    dernier_achat = fact_achats.groupby('id_client')['date_achat'].max().reset_index()
    dernier_achat.columns = ['id_client', 'dernier_achat']
    dernier_achat['dernier_achat'] = pd.to_datetime(dernier_achat['dernier_achat'])
    dernier_achat['jours_inactivite'] = (reference_date - dernier_achat['dernier_achat']).dt.days
    
    # Nombre d'achats par client
    nb_achats = fact_achats.groupby('id_client')['id_achat'].count().reset_index()
    nb_achats.columns = ['id_client', 'nb_achats']
    
    # Joindre
    retention = dernier_achat.merge(nb_achats, on='id_client')
    
    # Segmentation par statut
    retention['statut'] = retention.apply(
        lambda row: 'Actif' if row['jours_inactivite'] <= 30 
        else 'À risque' if row['jours_inactivite'] <= 90 
        else 'Inactif' if row['jours_inactivite'] <= 180 
        else 'Churn',
        axis=1
    )
    
    # Clients récurrents vs nouveaux
    retention['est_recurrent'] = retention['nb_achats'] > 1
    
    # Métriques globales
    total_clients = len(retention)
    clients_actifs = len(retention[retention['statut'] == 'Actif'])
    clients_recurrents = retention['est_recurrent'].sum()
    clients_churn = len(retention[retention['statut'] == 'Churn'])
    
    taux_retention_30j = (clients_actifs / total_clients * 100) if total_clients > 0 else 0
    taux_recurrence = (clients_recurrents / total_clients * 100) if total_clients > 0 else 0
    taux_churn = (clients_churn / total_clients * 100) if total_clients > 0 else 0
    
    # Rétention par période
    retention_periods = {}
    for days in [30, 60, 90, 180, 365]:
        clients_retained = len(retention[retention['jours_inactivite'] <= days])
        retention_periods[f'retention_{days}j'] = (clients_retained / total_clients * 100) if total_clients > 0 else 0
    
    # Résumé par statut
    retention_summary = retention.groupby('statut').agg({
        'id_client': 'count',
        'jours_inactivite': 'mean',
        'nb_achats': 'mean'
    }).reset_index()
    
    retention_summary.columns = ['statut', 'nb_clients', 'jours_inactivite_moyen', 'nb_achats_moyen']
    
    # Métriques globales
    global_metrics = pd.DataFrame([{
        'total_clients': total_clients,
        'clients_actifs': clients_actifs,
        'clients_recurrents': clients_recurrents,
        'clients_churn': clients_churn,
        'taux_retention_30j': taux_retention_30j,
        'taux_recurrence': taux_recurrence,
        'taux_churn': taux_churn,
        **retention_periods
    }])
    
    return {
        'retention_detail': retention,
        'retention_summary': retention_summary,
        'global_metrics': global_metrics
    }

