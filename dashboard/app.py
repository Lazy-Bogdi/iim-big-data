"""Dashboard Streamlit pour visualiser les données Gold"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import sys
import os

# Ajouter le chemin pour les imports
sys.path.append(os.path.dirname(__file__))

from utils.data_loader import (
    load_all_kpis,
    load_all_facts,
    load_all_analytics,
    load_all_kpis_api,
    load_all_facts_api,
    load_all_analytics_api,
)

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Analytics - Big Data",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)  # Cache pour 5 minutes
def load_data(source: str):
    """Charge toutes les données avec cache, soit depuis MinIO, soit via l'API MongoDB."""
    if source == "MinIO (direct)":
        with st.spinner("Chargement des données depuis Gold (MinIO)..."):
            kpis = load_all_kpis()
            facts = load_all_facts()
            analytics = load_all_analytics()
    else:
        with st.spinner("Chargement des données via l'API (MongoDB)..."):
            kpis = load_all_kpis_api()
            facts = load_all_facts_api()
            analytics = load_all_analytics_api()
    return kpis, facts, analytics


def main():
    """Application principale"""
    
    # Header
    st.markdown('<h1 class="main-header">📊 Dashboard Analytics</h1>', unsafe_allow_html=True)
    
    # Sidebar pour navigation
    st.sidebar.title("📑 Navigation")

    source = st.sidebar.radio(
        "Source des données",
        ["MinIO (direct)", "API Mongo"],
        index=0,
    )

    page = st.sidebar.selectbox(
        "Choisir une page",
        [
            "🏠 Accueil - KPIs Globaux",
            "📈 Évolution Temporelle",
            "🌍 Analyse Géographique",
            "🎯 Segmentation RFM",
            "💰 Customer Lifetime Value",
            "🔄 Rétention & Churn",
            "📦 Performance Produits",
            "📅 Saisonnalité",
            "📊 Analyses Avancées"
        ]
    )
    
    # Comparaison des temps dans la sidebar
    st.sidebar.divider()
    st.sidebar.subheader("⏱ Comparaison des temps")
    if st.sidebar.button("Mesurer MinIO vs API"):
        from utils.data_loader import benchmark_sources
        with st.sidebar.spinner("Mesure en cours..."):
            times = benchmark_sources()
        st.sidebar.metric("MinIO direct", f"{times['minio_total']:.3f} s")
        st.sidebar.metric("API Mongo", f"{times['api_total']:.3f} s")
        if times['api_total'] > 0:
            ratio = times['api_total'] / times['minio_total']
            st.sidebar.caption(f"API est {ratio:.2f}x {'plus lente' if ratio > 1 else 'plus rapide'} que MinIO")
    
    # Charger les données
    kpis, facts, analytics = load_data(source)
    
    # Router vers la bonne page
    if page == "🏠 Accueil - KPIs Globaux":
        show_home_page(kpis, facts)
    elif page == "📈 Évolution Temporelle":
        show_temporal_analysis(facts)
    elif page == "🌍 Analyse Géographique":
        show_geographic_analysis(facts, kpis)
    elif page == "🎯 Segmentation RFM":
        show_rfm_analysis(kpis)
    elif page == "💰 Customer Lifetime Value":
        show_clv_analysis(kpis)
    elif page == "🔄 Rétention & Churn":
        show_retention_analysis(kpis)
    elif page == "📦 Performance Produits":
        show_product_analysis(kpis)
    elif page == "📅 Saisonnalité":
        show_seasonality_analysis(analytics)
    elif page == "📊 Analyses Avancées":
        show_advanced_analytics(analytics)


def show_home_page(kpis: dict, facts: dict):
    """Page d'accueil avec KPIs globaux"""
    st.header("🏠 Vue d'ensemble")
    
    if kpis['globaux'].empty:
        st.error("⚠️ Aucune donnée disponible. Assurez-vous que le flow Gold a été exécuté.")
        return
    
    kpi_globaux = kpis['globaux'].iloc[0]
    
    # KPIs principaux en métriques
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="💰 CA Total",
            value=f"€{kpi_globaux['total_ca']:,.2f}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="🛒 Nombre d'achats",
            value=f"{int(kpi_globaux['nb_achats']):,}",
            delta=None
        )
    
    with col3:
        st.metric(
            label="👥 Clients",
            value=f"{int(kpi_globaux['nb_clients']):,}",
            delta=None
        )
    
    with col4:
        st.metric(
            label="💵 Panier moyen",
            value=f"€{kpi_globaux['panier_moyen']:.2f}",
            delta=None
        )
    
    st.divider()
    
    # Autres métriques
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📦 Produits", f"{int(kpi_globaux['nb_produits']):,}")
    with col2:
        st.metric("🌍 Pays", f"{int(kpi_globaux['nb_pays']):,}")
    with col3:
        st.metric("✅ Clients actifs (30j)", f"{int(kpi_globaux['clients_actifs_30j']):,}")
    with col4:
        st.metric("🆕 Nouveaux clients (30j)", f"{int(kpi_globaux['nouveaux_clients_30j']):,}")
    
    st.divider()
    
    # Graphique CA par mois
    if not facts['ca_mois'].empty:
        st.subheader("📈 Évolution du CA mensuel")
        ca_mois = facts['ca_mois'].copy()
        ca_mois['annee_mois'] = pd.to_datetime(ca_mois['annee_mois'])
        
        fig = px.line(
            ca_mois,
            x='annee_mois',
            y='ca_total',
            title="CA Total par Mois",
            labels={'ca_total': 'CA (€)', 'annee_mois': 'Mois'}
        )
        fig.update_traces(line_color='#1f77b4', line_width=3)
        st.plotly_chart(fig, use_container_width=True)
        
        # Tableau avec croissance
        st.subheader("📊 Détails mensuels")
        display_cols = ['annee_mois', 'ca_total', 'nb_clients_uniques', 'nb_achats', 'taux_croissance_mom']
        if all(col in ca_mois.columns for col in display_cols):
            st.dataframe(
                ca_mois[display_cols].round(2),
                use_container_width=True
            )


def show_temporal_analysis(facts: dict):
    """Analyse temporelle"""
    st.header("📈 Évolution Temporelle")
    
    # Sélection de la granularité
    granularite = st.selectbox(
        "Choisir la granularité",
        ["Par jour", "Par semaine", "Par mois", "Par heure"]
    )
    
    if granularite == "Par jour" and not facts['ca_jour'].empty:
        df = facts['ca_jour'].copy()
        df['date'] = pd.to_datetime(df['date'])
        x_col = 'date'
        title = "CA par Jour"
    elif granularite == "Par semaine" and not facts['ca_semaine'].empty:
        df = facts['ca_semaine'].copy()
        df['date_debut'] = pd.to_datetime(df['date_debut'])
        x_col = 'date_debut'
        title = "CA par Semaine"
    elif granularite == "Par mois" and not facts['ca_mois'].empty:
        df = facts['ca_mois'].copy()
        df['annee_mois'] = pd.to_datetime(df['annee_mois'])
        x_col = 'annee_mois'
        title = "CA par Mois"
    elif granularite == "Par heure" and not facts['ca_heure'].empty:
        df = facts['ca_heure'].copy()
        x_col = 'heure'
        title = "CA par Heure de la Journée"
    else:
        st.warning("Données non disponibles pour cette granularité")
        return
    
    # Graphique
    fig = px.line(
        df,
        x=x_col,
        y='ca_total',
        title=title,
        labels={'ca_total': 'CA (€)', x_col: granularite}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Métriques
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CA Total", f"€{df['ca_total'].sum():,.2f}")
    with col2:
        st.metric("CA Moyen", f"€{df['ca_total'].mean():,.2f}")
    with col3:
        st.metric("CA Max", f"€{df['ca_total'].max():,.2f}")


def show_geographic_analysis(facts: dict, kpis: dict):
    """Analyse géographique"""
    st.header("🌍 Analyse Géographique")
    
    if facts['ca_pays'].empty:
        st.warning("Données géographiques non disponibles")
        return
    
    df = facts['ca_pays'].copy()
    
    # Graphique en barres
    fig = px.bar(
        df.head(10),
        x='pays',
        y='ca_total',
        title="Top 10 Pays par CA",
        labels={'ca_total': 'CA (€)', 'pays': 'Pays'},
        color='ca_total',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Carte de chaleur (si on avait des coordonnées)
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Métriques par pays")
        display_cols = ['pays', 'ca_total', 'nb_clients_uniques', 'panier_moyen', 'pct_ca_total']
        if all(col in df.columns for col in display_cols):
            st.dataframe(
                df[display_cols].round(2),
                use_container_width=True
            )
    
    with col2:
        st.subheader("📈 Distribution")
        fig_pie = px.pie(
            df,
            values='ca_total',
            names='pays',
            title="Répartition du CA par Pays"
        )
        st.plotly_chart(fig_pie, use_container_width=True)


def show_rfm_analysis(kpis: dict):
    """Analyse RFM"""
    st.header("🎯 Segmentation RFM")
    
    if kpis['rfm'].empty:
        st.warning("Données RFM non disponibles")
        return
    
    df = kpis['rfm'].copy()
    
    # Graphique en barres par segment
    fig = px.bar(
        df,
        x='segment',
        y='nb_clients',
        title="Nombre de clients par segment RFM",
        labels={'nb_clients': 'Nombre de clients', 'segment': 'Segment'},
        color='nb_clients',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Tableau détaillé
    st.subheader("📊 Détails par segment")
    st.dataframe(df, use_container_width=True)


def show_clv_analysis(kpis: dict):
    """Analyse CLV"""
    st.header("💰 Customer Lifetime Value")
    
    if kpis['clv_pays'].empty:
        st.warning("Données CLV non disponibles")
        return
    
    df = kpis['clv_pays'].copy()
    
    # Graphique CLV par pays
    fig = px.bar(
        df,
        x='pays',
        y='clv_moyen',
        title="CLV Moyen par Pays",
        labels={'clv_moyen': 'CLV Moyen (€)', 'pays': 'Pays'},
        color='clv_moyen',
        color_continuous_scale='Greens'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Tableau
    st.dataframe(df, use_container_width=True)


def show_retention_analysis(kpis: dict):
    """Analyse de rétention"""
    st.header("🔄 Rétention & Churn")
    
    if kpis['retention_global'].empty:
        st.warning("Données de rétention non disponibles")
        return
    
    ret_global = kpis['retention_global'].iloc[0]
    ret_summary = kpis['retention_summary']
    
    # Métriques
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Taux de rétention (30j)", f"{ret_global['taux_retention_30j']:.2f}%")
    with col2:
        st.metric("Taux de récurrence", f"{ret_global['taux_recurrence']:.2f}%")
    with col3:
        st.metric("Taux de churn", f"{ret_global['taux_churn']:.2f}%")
    with col4:
        st.metric("Clients actifs", f"{int(ret_global['clients_actifs']):,}")
    
    # Graphique par statut
    if not ret_summary.empty:
        fig = px.bar(
            ret_summary,
            x='statut',
            y='nb_clients',
            title="Répartition des clients par statut",
            color='statut',
            color_discrete_map={
                'Actif': 'green',
                'À risque': 'orange',
                'Inactif': 'red',
                'Churn': 'darkred'
            }
        )
        st.plotly_chart(fig, use_container_width=True)


def show_product_analysis(kpis: dict):
    """Analyse produits"""
    st.header("📦 Performance Produits")
    
    if kpis['top_produits_ca'].empty:
        st.warning("Données produits non disponibles")
        return
    
    # Top produits par CA
    st.subheader("🏆 Top 10 Produits par CA")
    fig = px.bar(
        kpis['top_produits_ca'],
        x='produit',
        y='ca_total',
        title="Top 10 Produits",
        labels={'ca_total': 'CA (€)', 'produit': 'Produit'},
        color='ca_total',
        color_continuous_scale='Purples'
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Tableau complet
    if not kpis['produits'].empty:
        st.subheader("📊 Tous les produits")
        st.dataframe(kpis['produits'], use_container_width=True)


def show_seasonality_analysis(analytics: dict):
    """Analyse de saisonnalité"""
    st.header("📅 Saisonnalité")
    
    # Par jour de la semaine
    if not analytics['saisonnalite_jour'].empty:
        st.subheader("📆 Par jour de la semaine")
        df = analytics['saisonnalite_jour'].copy()
        fig = px.bar(
            df,
            x='jour_semaine',
            y='ca_total',
            title="CA par Jour de la Semaine",
            labels={'ca_total': 'CA (€)', 'jour_semaine': 'Jour'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Par heure
    if not analytics['saisonnalite_heure'].empty:
        st.subheader("🕐 Par heure de la journée")
        df = analytics['saisonnalite_heure'].copy()
        fig = px.line(
            df,
            x='heure',
            y='ca_total',
            title="CA par Heure",
            labels={'ca_total': 'CA (€)', 'heure': 'Heure'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Par mois
    if not analytics['saisonnalite_mois'].empty:
        st.subheader("📅 Par mois")
        df = analytics['saisonnalite_mois'].copy()
        fig = px.bar(
            df,
            x='mois_nom',
            y='ca_total',
            title="CA par Mois",
            labels={'ca_total': 'CA (€)', 'mois_nom': 'Mois'}
        )
        st.plotly_chart(fig, use_container_width=True)


def show_advanced_analytics(analytics: dict):
    """Analyses avancées"""
    st.header("📊 Analyses Avancées")
    
    # Concentration
    if not analytics['concentration_summary'].empty:
        st.subheader("📈 Métriques de concentration")
        conc = analytics['concentration_summary'].iloc[0]
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Indice Gini (Clients)", f"{conc['indice_gini_clients']:.3f}")
        with col2:
            st.metric("% CA Top 10% Clients", f"{conc['pct_ca_top_10_clients']:.2f}%")
        with col3:
            st.metric("% CA Top 20% Clients", f"{conc['pct_ca_top_20_clients']:.2f}%")
    
    # Cohortes
    if not analytics['cohortes_total'].empty:
        st.subheader("👥 Analyse par cohortes")
        st.dataframe(analytics['cohortes_total'], use_container_width=True)


if __name__ == "__main__":
    main()


