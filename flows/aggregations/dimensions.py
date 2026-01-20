"""Création des tables de dimensions pour la couche Gold"""

import pandas as pd
from typing import Dict


def create_dim_clients(clients_df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée la dimension clients enrichie.
    
    Args:
        clients_df: DataFrame clients depuis Silver
    
    Returns:
        DataFrame dimension clients
    """
    dim_clients = clients_df.copy()
    
    # Ajouter des champs calculés
    dim_clients['annee_inscription'] = pd.to_datetime(dim_clients['date_inscription']).dt.year
    dim_clients['mois_inscription'] = pd.to_datetime(dim_clients['date_inscription']).dt.month
    dim_clients['trimestre_inscription'] = pd.to_datetime(dim_clients['date_inscription']).dt.quarter
    
    return dim_clients


def create_dim_produits(achats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Crée la dimension produits.
    
    Args:
        achats_df: DataFrame achats depuis Silver
    
    Returns:
        DataFrame dimension produits
    """
    dim_produits = achats_df.groupby('produit').agg({
        'montant': ['min', 'max', 'mean', 'sum'],
        'id_achat': 'count'
    }).reset_index()
    
    dim_produits.columns = [
        'produit',
        'montant_min',
        'montant_max',
        'montant_moyen',
        'ca_total',
        'nb_achats'
    ]
    
    # Catégoriser les produits par prix
    dim_produits['categorie_prix'] = pd.cut(
        dim_produits['montant_moyen'],
        bins=[0, 100, 250, 500, float('inf')],
        labels=['Bas', 'Moyen', 'Élevé', 'Premium']
    )
    
    return dim_produits


def create_dim_temps(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    """
    Crée la dimension temps (calendrier).
    
    Args:
        start_date: Date de début
        end_date: Date de fin
    
    Returns:
        DataFrame dimension temps
    """
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dim_temps = pd.DataFrame({
        'date': dates,
        'jour': dates.day,
        'mois': dates.month,
        'annee': dates.year,
        'trimestre': dates.quarter,
        'semaine_annee': dates.isocalendar().week,
        'jour_semaine': dates.day_name(),
        'jour_semaine_num': dates.dayofweek,
        'est_weekend': dates.dayofweek.isin([5, 6]),
        'mois_nom': dates.strftime('%B'),
        'trimestre_nom': dates.to_period('Q').astype(str),
    })
    
    return dim_temps

