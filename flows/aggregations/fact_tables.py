"""Création des tables de faits pour la couche Gold"""

import pandas as pd
from typing import Tuple


def create_fact_achats(achats_df: pd.DataFrame, clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée la table de faits principale FACT_ACHATS avec enrichissement.
    
    Args:
        achats_df: DataFrame achats depuis Silver
        clients_df: DataFrame clients depuis Silver
    
    Returns:
        DataFrame table de faits enrichie
    """
    # Joindre achats avec clients
    fact_achats = achats_df.merge(
        clients_df[['id_client', 'pays', 'date_inscription']],
        on='id_client',
        how='left'
    )
    
    # Enrichir avec informations temporelles
    fact_achats['date_achat'] = pd.to_datetime(fact_achats['date_achat'])
    fact_achats['annee'] = fact_achats['date_achat'].dt.year
    fact_achats['mois'] = fact_achats['date_achat'].dt.month
    fact_achats['trimestre'] = fact_achats['date_achat'].dt.quarter
    fact_achats['semaine_annee'] = fact_achats['date_achat'].dt.isocalendar().week
    fact_achats['jour_semaine'] = fact_achats['date_achat'].dt.day_name()
    fact_achats['jour_semaine_num'] = fact_achats['date_achat'].dt.dayofweek
    fact_achats['heure'] = fact_achats['date_achat'].dt.hour
    fact_achats['est_weekend'] = fact_achats['date_achat'].dt.dayofweek.isin([5, 6])
    
    # Calculer l'ancienneté du client au moment de l'achat
    fact_achats['date_inscription'] = pd.to_datetime(fact_achats['date_inscription'])
    fact_achats['anciennete_jours'] = (
        fact_achats['date_achat'] - fact_achats['date_inscription']
    ).dt.days
    
    # Ajouter des champs calculés
    fact_achats['annee_mois'] = fact_achats['date_achat'].dt.to_period('M').astype(str)
    fact_achats['annee_semaine'] = (
        fact_achats['annee'].astype(str) + '-W' + 
        fact_achats['semaine_annee'].astype(str).str.zfill(2)
    )
    
    return fact_achats

