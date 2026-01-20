# Projet Big Data – Pipeline ELT, Analytics & NoSQL

Pipeline de données complète pour un cas e-commerce avec architecture **Bronze / Silver / Gold**, orchestration **Prefect**, stockage **MinIO**, **MongoDB**, **API FastAPI** et **dashboard Streamlit**.

---

## 🚀 Setup du projet (après git clone/pull)

### 1. Prérequis

- **Python 3.12**
- **Docker** + **docker-compose**
- **Git**

### 2. Installation

```bash
# Cloner le repo (si pas déjà fait)
git clone <url-du-repo>
cd big-data

# Créer l'environnement virtuel
python3 -m venv .venv
source .venv/bin/activate  # Sur Windows: .venv\Scripts\activate

# Installer les dépendances
pip install --upgrade pip
pip install -r requirements.txt
pip install -r api/requirements.txt
```

### 3. Démarrer l'infrastructure Docker

```bash
# Démarrer MinIO, Prefect, Postgres, MongoDB, Mongo Express
docker-compose up -d

# Vérifier que tout est démarré
docker ps
```

**Services disponibles :**
- **MinIO** : `http://localhost:9000` (Console: `http://localhost:9001`, user: `minioadmin` / `minioadmin`)
- **Prefect UI** : `http://localhost:4200`
- **MongoDB** : `localhost:27017`
- **Mongo Express** : `http://localhost:8081` (user: `admin` / `admin`)

### 4. Générer les données de test

```bash
python scripts/generate_data.py
```

Cela crée `data/sources/clients.csv` et `data/sources/achats.csv`.

### 5. Exécuter la pipeline ELT complète

```bash
# 1. Ingestion Bronze (CSV -> MinIO bronze)
python flows/bronze_ingestion.py

# 2. Transformation Silver (nettoyage, normalisation)
python flows/silver_transformation.py

# 3. Agrégations Gold (KPIs, dimensions, faits)
python flows/gold_aggregation.py
```

### 6. Synchroniser Gold → MongoDB

```bash
python -m mongo_pipeline.mongo_writer
```

### 7. Lancer l'API FastAPI

```bash
# Dans un terminal séparé
uvicorn api.app:app --reload --port 8000
```

**API disponible :** `http://localhost:8000`
- Docs Swagger : `http://localhost:8000/docs`
- Endpoints : `/kpis/{name}`, `/facts/{name}`, `/analytics/{name}`, `/meta/last-refresh`

### 8. Lancer le dashboard Streamlit

```bash
# Dans un autre terminal
streamlit run dashboard/app.py
```

**Dashboard disponible :** `http://localhost:8501`

Dans la sidebar, tu peux :
- Choisir la source : **"MinIO (direct)"** ou **"API Mongo"**
- Mesurer les temps de chargement avec le bouton **"Mesurer MinIO vs API"**

---

## 📋 Ce qui a été implémenté

### ✅ 1. Pipeline ELT robuste (Bronze / Silver / Gold)

**Objectif :** Ingestion, nettoyage et agrégation des données e-commerce.

**Fonctionnalités :**
- **Bronze** : Ingestion générique (découverte automatique de tous les CSV dans `data/sources/`)
- **Silver** : Nettoyage intelligent générique + cleaners spécifiques pour `clients`/`achats`
  - Gestion des valeurs nulles/aberrantes
  - Standardisation des dates
  - Normalisation des types
  - Déduplication
  - Contrôles de qualité (complétude, unicité, validité)
- **Gold** : Agrégations métier complètes
  - Dimensions : `dim_clients`, `dim_produits`, `dim_temps`, `dim_rfm`
  - Faits : CA par jour/semaine/mois/heure/pays
  - KPIs : CLV, rétention, churn, RFM, croissance, performance produits
  - Analytics : saisonnalité, concentration (Pareto/Gini), cohortes

**Fichiers clés :**
- `flows/bronze_ingestion.py`
- `flows/silver_transformation.py`
- `flows/gold_aggregation.py`
- `flows/transformations/` (nettoyage)
- `flows/aggregations/` (agrégations)

---

### ✅ 2. Machine Learning pour enrichissement automatique

**Objectif :** Extraire automatiquement plus d'informations sans règles métier explicites.

**Fonctionnalités :**
- **Feature engineering automatique** : détection de patterns (dates, numériques, catégorielles)
- **Détection d'anomalies** : Isolation Forest (10% d'anomalies détectées)
- **Clustering automatique** : K-Means pour segmentation (10 clusters)
- **Scores prédits** : score composite (0-100) par client/achat

**Fichiers clés :**
- `flows/ml/feature_engineering.py`
- `flows/ml/ml_models.py`
- Intégré dans `flows/gold_aggregation.py`

**Résultat :** Les données Gold sont enrichies avec des colonnes ML (`is_anomaly_ml`, `ml_cluster`, `ml_score`) sans casser les KPIs existants.

---

### ✅ 3. Base NoSQL opérationnelle avec MongoDB

**Objectif :** Pipeline Gold → MongoDB + API + Dashboard avec comparaison des temps.

**Fonctionnalités :**
- **Pipeline MongoDB** (`mongo_pipeline/mongo_writer.py`) :
  - Lit tous les Parquet Gold depuis MinIO
  - Écrit dans des collections Mongo (`kpis_*`, `facts_*`, `analytics_*`)
  - Mesure et stocke le temps de refresh Gold → Mongo
- **API FastAPI** (`api/app.py`) :
  - Endpoints REST : `/kpis/{name}`, `/facts/{name}`, `/analytics/{name}`
  - Endpoint métadonnées : `/meta/last-refresh`
- **Dashboard Streamlit** :
  - **Switch de source** : MinIO direct OU API Mongo (mêmes données)
  - **Comparaison des temps** : bouton dans la sidebar pour mesurer MinIO vs API
  - Aucun changement de logique métier, juste deux modes de lecture

**Fichiers clés :**
- `mongo_pipeline/mongo_writer.py`
- `api/app.py`
- `dashboard/utils/data_loader.py` (deux modes : MinIO + API)
- `dashboard/app.py` (switch dans la sidebar)

**Résultat :** Tu peux comparer directement dans Streamlit les temps de chargement MinIO vs API Mongo pour exactement les mêmes données.

---

### ✅ 4. Dashboard Streamlit complet

**Objectif :** Visualisation interactive de tous les KPIs et analyses.

**Pages disponibles :**
- 🏠 **Accueil** : KPIs globaux (CA, clients, achats, panier moyen)
- 📈 **Évolution temporelle** : CA par jour/semaine/mois/heure
- 🌍 **Analyse géographique** : CA par pays, top 10, distribution
- 🎯 **Segmentation RFM** : Clients par segment (Champions, Loyal, etc.)
- 💰 **Customer Lifetime Value** : CLV moyen par pays
- 🔄 **Rétention & Churn** : Taux de rétention, récurrence, churn
- 📦 **Performance produits** : Top produits par CA, volume
- 📅 **Saisonnalité** : Patterns par jour/heure/mois de la semaine
- 📊 **Analyses avancées** : Concentration (Gini), cohortes

**Fichiers clés :**
- `dashboard/app.py`
- `dashboard/utils/data_loader.py`

---

## 🏗️ Architecture technique

### Stack

- **Python 3.12** + **Pandas** + **PyArrow** (traitement batch)
- **Prefect** (orchestration)
- **MinIO** (data lake S3-compatible)
- **MongoDB 7** (base NoSQL opérationnelle)
- **FastAPI** (API REST)
- **Streamlit** (dashboard)
- **scikit-learn** (ML)
- **Docker Compose** (infrastructure)

### Structure des données

```
MinIO (data lake)
├── sources/     # CSV bruts
├── bronze/      # Données brutes (CSV)
├── silver/      # Données nettoyées (Parquet)
└── gold/        # Données agrégées (Parquet)
    ├── dimensions/
    ├── facts/
    ├── kpis/
    ├── analytics/
    └── ml/       # Données enrichies ML

MongoDB
├── kpis_*       # Collections KPIs
├── facts_*      # Collections faits
├── analytics_*  # Collections analytics
└── metadata_refresh  # Métadonnées de refresh
```

---



## (non implémenté)

- **Kafka + Spark Streaming** : Streaming temps réel avec détection d'anomalies
- **Metabase** : Dashboard BI connecté à MongoDB (bonus)

---


