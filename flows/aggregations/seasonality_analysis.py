"""Analyse de saisonnalité pour la couche Gold"""

import pandas as pd


def calculate_seasonality(fact_achats: pd.DataFrame) -> dict:
    """
    Calcule les patterns de saisonnalité.
    
    Args:
        fact_achats: Table de faits
    
    Returns:
        Dict avec analyses de saisonnalité
    """
    # Par jour de la semaine
    seasonality_dow = fact_achats.groupby('jour_semaine').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    seasonality_dow.columns = [
        'jour_semaine',
        'ca_total',
        'ca_moyen',
        'nb_jours',
        'nb_clients_uniques',
        'nb_achats'
    ]
    
    # Ordre des jours
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    seasonality_dow['jour_order'] = seasonality_dow['jour_semaine'].apply(lambda x: day_order.index(x) if x in day_order else 99)
    seasonality_dow = seasonality_dow.sort_values('jour_order')
    
    # Par heure de la journée
    seasonality_hour = fact_achats.groupby('heure').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    seasonality_hour.columns = [
        'heure',
        'ca_total',
        'ca_moyen',
        'nb_heures',
        'nb_clients_uniques',
        'nb_achats'
    ]
    
    # Par mois
    seasonality_month = fact_achats.groupby('mois').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    seasonality_month.columns = [
        'mois',
        'ca_total',
        'ca_moyen',
        'nb_mois',
        'nb_clients_uniques',
        'nb_achats'
    ]
    
    # Noms des mois
    month_names = {1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril',
                   5: 'Mai', 6: 'Juin', 7: 'Juillet', 8: 'Août',
                   9: 'Septembre', 10: 'Octobre', 11: 'Novembre', 12: 'Décembre'}
    seasonality_month['mois_nom'] = seasonality_month['mois'].map(month_names)
    
    # Weekend vs semaine
    seasonality_weekend = fact_achats.groupby('est_weekend').agg({
        'montant': ['sum', 'mean', 'count'],
        'id_client': 'nunique',
        'id_achat': 'count'
    }).reset_index()
    
    seasonality_weekend.columns = [
        'est_weekend',
        'ca_total',
        'ca_moyen',
        'nb_periodes',
        'nb_clients_uniques',
        'nb_achats'
    ]
    seasonality_weekend['type'] = seasonality_weekend['est_weekend'].apply(lambda x: 'Weekend' if x else 'Semaine')
    
    return {
        'by_day_of_week': seasonality_dow,
        'by_hour': seasonality_hour,
        'by_month': seasonality_month,
        'weekend_vs_week': seasonality_weekend
    }

