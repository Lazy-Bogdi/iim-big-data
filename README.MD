## Projet Big Data – Pipeline ELT, Analytics & Temps Réel

Ce projet met en place une **pipeline de données complète** autour d’un cas e‑commerce (clients, achats) avec :
- une architecture **Bronze / Silver / Gold**,
- de l’orchestration avec **Prefect**,
- du stockage objet avec **MinIO**,
- un **dashboard Streamlit** pour la visualisation,
- et (à implémenter étape par étape) : **Spark**, **MongoDB**, **API FastAPI/Flask** et **Kafka + Spark Streaming**.

---

## 1. Objectifs pédagogiques (consignes du projet)

### 1.1 Amélioration de la pipeline
- Rendre la pipeline plus **robuste** à l’ingestion de nouvelles données.
- Mettre en place du **machine learning** pour extraire plus d’informations (choix du modèle libre).

### 1.2 Big Data authentique avec Spark
- Remplacer **pandas** par **PySpark** dans la pipeline ELT existante.
- Ajouter un **cluster Spark** (1 master + 2 workers via Docker Compose).
- Convertir les transformations Pandas en transformations **Spark**.
- Comparer les **temps de traitement** Pandas vs Spark.

### 1.3 Base NoSQL opérationnelle avec MongoDB
- Pipeline qui **lit la couche Gold** (Parquet) et écrit dans **MongoDB**.
- **API Flask/FastAPI** qui expose les données MongoDB.
- Dashboard **Streamlit** qui interroge l’API.
- Calculer le **temps de refresh** entre la mise à jour MongoDB et la mise à jour du dashboard.
- Bonus : mise en place de **Metabase** pour un dashboard BI.

### 1.4 Streaming temps réel avec Kafka + Spark Streaming
- Producteur **Kafka** qui simule des événements e‑commerce (clics, achats, paniers abandonnés…).
- **Spark Streaming** qui consomme Kafka, applique des transformations et détecte des anomalies.

---

## 2. Architecture globale du projet

### 2.1 Arborescence principale

```text
big-data/
├── api/                    # (À implémenter) API Flask/FastAPI exposant MongoDB
├── dashboard/              # Dashboard Streamlit (couche Gold)
├── data/
│   └── sources/            # Données brutes CSV générées
├── flows/
│   ├── bronze_ingestion.py # Ingestion vers MinIO (sources -> bronze)
│   ├── silver_transformation.py # Nettoyage / normalisation (bronze -> silver)
│   ├── gold_aggregation.py # Agrégations métier & KPIs (silver -> gold)
│   ├── config.py           # Config MinIO / Prefect / buckets
│   ├── transformations/    # Fonctions de nettoyage & qualité (Silver)
│   ├── aggregations/       # Logique d’agrégation (Gold)
│   ├── spark/              # (À implémenter) Versions PySpark de Silver/Gold
│   └── streaming/          # (À implémenter) Kafka producer & Spark Streaming
├── mongo_pipeline/         # (À implémenter) Écriture Gold -> MongoDB
├── scripts/
│   └── generate_data.py    # Génération des CSV de test (clients, achats)
├── services/               # (À implémenter) Docker Compose dédiés Spark/Mongo/Kafka
├── docker-compose.yml      # MinIO + Prefect + Postgres (orchestration)
├── requirements.txt        # Dépendances Python
├── consignes.txt           # Consignes du projet (copie du sujet)
└── README.MD               # (ce fichier)
```

### 2.2 Stack technique

- **Python** 3.12
- **Prefect** : orchestration des flows
- **MinIO** : data lake S3‑compatible (buckets `sources`, `bronze`, `silver`, `gold`)
- **Pandas / PyArrow** : traitement batch (actuel, Silver & Gold)
- **Streamlit** : dashboard interactif (`dashboard/app.py`) – cf. [docs Streamlit](https://streamlit.io/)
- **Docker Compose** : MinIO, Prefect Server, Postgres
- (À venir) **PySpark**, **MongoDB**, **FastAPI/Flask**, **Kafka**, **Spark Streaming**

---

## 3. Pipeline ELT actuelle (Bronze / Silver / Gold)

### 3.1 Génération des données de test

Script : `scripts/generate_data.py`
- Génère :
  - `data/sources/clients.csv`
  - `data/sources/achats.csv`
- Colonnes principales :
  - **clients** : `id_client`, `nom`, `email`, `date_inscription`, `pays`
  - **achats** : `id_achat`, `id_client`, `date_achat`, `montant`, `produit`

Commande :

```bash
python scripts/generate_data.py
```

### 3.2 Couche Bronze – Ingestion MinIO

Flow : `flows/bronze_ingestion.py`
- Upload des CSV locaux (`data/sources/*.csv`) vers le bucket MinIO `sources`.
- Copie des fichiers du bucket `sources` vers le bucket `bronze`.
- Orchestration Prefect avec tâches `upload_to_sources` et `copy_to_bronze_layer`.

Commande :

```bash
docker-compose up -d          # démarre MinIO, Prefect, Postgres
python flows/bronze_ingestion.py
```

Résultat :
- `sources/clients.csv`, `sources/achats.csv` dans MinIO.
- `bronze/clients.csv`, `bronze/achats.csv` dans MinIO.

### 3.3 Couche Silver – Nettoyage & normalisation

Flow : `flows/silver_transformation.py`
- Lecture des CSV depuis `bronze/`.
- Nettoyage & normalisation avec `flows/transformations/` :
  - Gestion des valeurs manquantes / aberrantes.
  - Standardisation des dates.
  - Normalisation des types.
  - Déduplication.
  - Contrôles de qualité (complétude, unicité, validité) + rapport.
- Écriture en Parquet vers le bucket `silver/` :
  - `silver/clients.parquet`
  - `silver/achats.parquet`

Commande :

```bash
python flows/silver_transformation.py
```

### 3.4 Couche Gold – Agrégations & KPIs

Flow : `flows/gold_aggregation.py`
- Lecture de `silver/clients.parquet` et `silver/achats.parquet`.
- Construction :
  - **Dimensions** : `dim_clients`, `dim_produits`, `dim_temps`, `dim_rfm`.
  - **Faits** : `fact_achats`, `fact_ca_jour`, `fact_ca_semaine`, `fact_ca_mois`, `fact_ca_heure`, `fact_ca_pays`.
  - **KPIs** : CLV, rétention, RFM, performance produits, croissance, etc.
  - **Analyses** : saisonnalité, concentration (Pareto / Gini), cohortes.
- Écriture en Parquet vers le bucket `gold/` dans les sous‑dossiers :
  - `gold/dimensions/`, `gold/facts/`, `gold/kpis/`, `gold/analytics/`.

Commande :

```bash
python flows/gold_aggregation.py
```

---

## 4. Dashboard Streamlit (couche Gold)

App : `dashboard/app.py` (+ utilitaires dans `dashboard/utils/data_loader.py`)

- Charge les Parquet de la couche Gold directement depuis MinIO (bucket `gold`).
- Pages disponibles :
  - **Accueil – KPIs globaux**.
  - **Évolution temporelle** (jour/semaine/mois/heure).
  - **Analyse géographique** (CA par pays).
  - **Segmentation RFM**.
  - **Customer Lifetime Value (CLV)**.
  - **Rétention & churn**.
  - **Performance produits**.
  - **Saisonnalité**.
  - **Analyses avancées** (concentration, cohortes, etc.).

Lancement :

```bash
streamlit run dashboard/app.py
```

Dashboard disponible sur : `http://localhost:8501`

---

## 5. Organisation du code & bonnes pratiques Git

### 5.1 Ce qui est versionné

- Code Python (`flows/`, `dashboard/`, `scripts/`, `api/`, `mongo_pipeline/`…).
- Fichiers de config : `docker-compose.yml`, `requirements.txt`, `README.MD`, `consignes.txt`.
- Structure des dossiers de données (via éventuels `.gitkeep` si besoin).

### 5.2 Ce qui n’est **pas** versionné (voir `.gitignore`)

- **Environnements & artefacts Python** :
  - `.venv/`, `__pycache__/`, `*.pyc`, etc.
- **Secrets & config locale** :
  - `.env`, `.env.local`, `.streamlit/secrets.toml`.
- **Données** :
  - `data/sources/*.csv`, `data/database/`, `data/raw/`, `data/processed/`.
  - `*.parquet`, `*.csv` globaux (sauf éventuels `.gitkeep`).
- **Systèmes & services** :
  - Volumes MinIO, Spark, MongoDB, Kafka (`minio_data/`, `spark-warehouse/`, `mongodb_data/`, `kafka_data/`…).
- **Caches & logs** :
  - `.pytest_cache/`, `.coverage`, `*.log`, etc.

Cela garantit un **repo propre**, sans données lourdes ni secrets.

---

## 6. Roadmap par rapport aux consignes

Les dossiers **existent déjà** mais les fichiers sont **encore vides** pour éviter le « code mort » : ils seront remplis étape par étape.

### 6.1 Robustesse & Machine Learning (prochaine étape)

Objectifs :
- Rendre la pipeline plus **robuste** :
  - Meilleure gestion des erreurs (ingestion, parsing CSV, schémas évolutifs).
  - Contrôles de qualité renforcés et centralisés.
  - Tests unitaires simples sur les fonctions de transformation.
- Ajouter un **bloc ML** (par exemple) :
  - Segmentation avancée clients (clustering sur features RFM / CA / fréquence).
  - Modèle simple de prédiction de churn ou d’upsell.

Implémentation prévue :
- Nouveau module `ml/` (ou intégration dans `flows/gold_aggregation.py`) qui :
  - Lit des features depuis la couche Gold.
  - Entraîne un modèle.
  - Écrit les scores (ex: `score_churn`, `segment_ml`) dans une nouvelle table Gold.

### 6.2 Big Data avec Spark (dossiers déjà créés)

Répertoire : `flows/spark/`
- `silver_spark.py` : version PySpark du flow Silver.
- `gold_spark.py` : version PySpark du flow Gold.

Répertoire : `services/spark/`
- Fichier à créer : `docker-compose-spark.yml` (cluster Spark 1 master + 2 workers).

Étapes prévues :
1. Créer le cluster Spark via Docker Compose.
2. Reprendre les transformations Pandas (Silver/Gold) et les réécrire en **PySpark DataFrame**.
3. Mesurer et comparer les **temps de traitement** Pandas vs Spark.

### 6.3 Base NoSQL avec MongoDB

Répertoire : `mongo_pipeline/`
- `mongo_writer.py` : pipeline qui lit des Parquet Gold et écrit dans MongoDB.

Répertoire : `api/`
- `app.py` : API Flask/FastAPI qui expose les données MongoDB.
- `requirements.txt` : dépendances API.

Répertoire : `services/mongodb/`
- Fichier à créer : `docker-compose-mongo.yml` (MongoDB + éventuellement Mongo Express).

Étapes prévues :
1. Lire certaines tables Gold (`fact_achats`, `dim_clients`…).
2. Les projeter dans des collections Mongo (schémas adaptés à la lecture API).
3. Exposer des endpoints REST (`/clients`, `/achats`, `/kpis`…).
4. Ajouter dans le dashboard Streamlit une page qui **interroge l’API** (et non MinIO directement).
5. Mesurer le **temps de refresh** entre Gold → Mongo → API → Dashboard.

### 6.4 Streaming temps réel (Kafka + Spark Streaming)

Répertoire : `flows/streaming/`
- `kafka_producer.py` : producteur Kafka simulant des événements e‑commerce.
- `spark_streaming.py` : job Spark Streaming qui consomme Kafka et détecte des anomalies.

Répertoire : `services/kafka/`
- Fichier à créer : `docker-compose-kafka.yml` (Kafka + Zookeeper ou stack moderne type Bitnami/Redpanda).

Étapes prévues :
1. Définir le schéma d’événements (clics, ajout panier, achat, abandon).
2. Implémenter un producteur Kafka simple.
3. Implémenter un job Spark Streaming pour :
   - agrégations temps réel (par exemple nombre d’événements par type/pays),
   - détection d’anomalies simples (pics de paniers abandonnés, etc.).
4. Option : exposer ces métriques en temps quasi réel (par ex. via MongoDB ou un autre sink).

---

## 7. Comment lancer le projet (état actuel)

### 7.1 Prérequis
- Python 3.12
- `docker` + `docker-compose`

### 7.2 Installation Python

```bash
cd big-data
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 7.3 Démarrer l’infrastructure de base (MinIO + Prefect + Postgres)

```bash
docker-compose up -d
```

### 7.4 Exécuter la pipeline ELT

```bash
# 1. Générer les données
python scripts/generate_data.py

# 2. Ingestion Bronze
python flows/bronze_ingestion.py

# 3. Transformation Silver
python flows/silver_transformation.py

# 4. Agrégations Gold
python flows/gold_aggregation.py
```

### 7.5 Lancer le dashboard Streamlit

```bash
streamlit run dashboard/app.py
```

---

## 8. Format de rendu (rappel)

Comme indiqué dans `consignes.txt` :
1. Envoyer un email à `nathan.vidal@ext.devinci.fr` avec le **lien du repo GitHub** du projet.
2. Déposer le projet **zippé** dans le dépôt prévu sur DVL.

---

Ce README sera mis à jour au fur et à mesure que chaque partie (Spark, MongoDB, Kafka, ML) sera implémentée.  
L’objectif est de garder **un repo propre**, sans code mort, avec une architecture claire et documentée à chaque étape.


