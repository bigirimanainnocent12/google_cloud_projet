# NYC Taxi Data Pipeline

Architecture ELT industrielle sur GCP — Ingestion, chargement et transformation de plus de 150 millions de trajets NYC TLC pour l'analyse predictive et le pilotage strategique sur Power BI.

Stack : Python 3.10+ | Google Cloud Platform | BigQuery | Cloud Composer (Airflow) | Power BI

---

## Presentation

Pipeline ELT de bout en bout qui ingere automatiquement les fichiers Parquet officiels de la NYC TLC, les stream directement vers Google Cloud Storage, les charge dans BigQuery, applique des transformations de qualite des donnees, et expose 15 vues analytiques sur 3 domaines metier — consommees par un dashboard Power BI interactif.

```
NYC TLC API (Parquet)
        |
        v
extraction_storage.py  -->  Google Cloud Storage (Zone brute)
                                        |
                                        v
                          chargement_bigquery.py  -->  BigQuery (rowyellontrips.trips)
                                                                    |
                                                                    v
                                                       transformation.py  -->  BigQuery (rowyellontripsclean)
                                                                                          |
                                                                                          v
                                                                                  Vues SQL (15 questions)
                                                                                          |
                                                                                          v
                                                                                      Power BI

Toutes les etapes sont orchestrees par Cloud Composer (Apache Airflow)
```

---

## Structure du depot

```
nyc-taxi-data-pipeline/
|
|-- chiens/
|   `-- workflow.py                        # DAG Airflow — orchestration du pipeline complet
|
|-- scripts/
|   |-- extraction_storage.py              # Extraction NYC TLC -> streaming vers GCS
|   |-- chargement_bigquery.py             # Chargement GCS -> BigQuery (table brute)
|   `-- transformation.py                 # Nettoyage et transformation BigQuery
|
`-- SQL/
    |-- CompetitiveInsights.sql            # Vues : efficacite operationnelle & zones
    |-- Financial_and_Pricing.sql          # Vues : revenus, tarifs, pourboires
    `-- MarketDemand_and_customer.sql      # Vues : demande, saisonnalite, comportement client
```

---

## Architecture & Datasets BigQuery

| Dataset                       | Table / Vue           | Description                                  |
|-------------------------------|-----------------------|----------------------------------------------|
| rowyellontrips                | trips                 | Table brute — toutes les courses chargees    |
| rowyellontrips                | Zonnes_taxis          | Referentiel des zones et boroughs NYC        |
| rowyellontripsclean           | transforme_data_trips | Table nettoyee (filtres qualite appliques)   |
| Wieusrowyellontripsdashboards | 15 vues SQL           | Vues analytiques exposees a Power BI         |

---

## Pipeline — Etape par etape

### 1. extraction_storage.py — Extraction et stockage

Telecharge en streaming direct (sans stockage local) les fichiers Parquet officiels de la NYC TLC de 2021 a l'annee en cours, puis les uploade vers Google Cloud Storage.

- Verification d'existence avant telechargement (idempotence)
- Upload resumable chunk par chunk (1 Mo)
- Logs automatiquement archives dans GCS (fromgit/logs/)

Destination GCS :

```
gs://{PROJET_ID}-data-bucket/dataset/trips/yellow_tripdata_AAAA-MM.parquet
```

---

### 2. chargement_bigquery.py — Chargement BigQuery

Compare les fichiers presents dans GCS avec les donnees deja chargees dans BigQuery via la colonne `Source_data`, et ne charge que les nouveaux fichiers (chargement incremental).

- Detection automatique du schema (autodetect)
- Ajout de la colonne `Source_data` pour la tracabilite
- Passage par une table temporaire avant insertion dans la table principale
- Suppression automatique de la table temporaire apres chaque chargement
- Logs archives dans GCS

```
GCS  -->  trips_temp (autodetect + Source_data)  -->  INSERT INTO trips  -->  DROP trips_temp
```

---

### 3. transformation.py — Nettoyage des donnees

Cree ou remplace la table `transforme_data_trips` a partir de la table brute en appliquant des filtres de qualite des donnees.

```sql
WHERE passenger_count > 0
  AND trip_distance > 0
  AND payment_type <> 6
  AND total_amount > 0
```

---

## Vues analytiques SQL (15 questions)

### Demande du marche et comportement client — MarketDemand_and_customer.sql

| Vue                        | Question analytique                                                   |
|----------------------------|-----------------------------------------------------------------------|
| demand_over_time           | Evolution quotidienne / hebdo / mensuelle / saisonniere de la demande |
| peak_hours_by_zone         | Heures de pointe par borough et zone                                  |
| popular_pickup_dropoff     | Lieux de prise en charge et de depose les plus populaires             |
| avg_trip_distance_analysis | Distance moyenne par borough, heure et saison                         |
| passenger_trends_by_season | Trajets solo vs. multi-passagers selon les saisons                    |

### Analyse financiere et tarification — Financial_and_Pricing.sql

| Vue                          | Question analytique                                                 |
|------------------------------|---------------------------------------------------------------------|
| total_fare_revenue_over_time | Evolution des revenus totaux (tarifs, pourboires, peages, congestion)|
| avg_fare_analysis            | Tarif moyen par borough, heure et distance                          |
| payment_type_trends          | Repartition et evolution des types de paiement                      |
| tipping_behavior_analysis    | Frequence et facteurs influencant les pourboires (carte uniquement) |
| additional_charges_revenue   | Revenus taxe MTA, surcharge congestion, frais aeroport              |

### Analyse concurrentielle et efficacite operationnelle — CompetitiveInsights.sql

| Vue                    | Question analytique                                                  |
|------------------------|----------------------------------------------------------------------|
| trip_volume_by_borough | Volumes de trajets les plus eleves et les plus faibles par zone      |
| airport_trips_analysis | Frequence et tarif moyen des trajets aeroports (JFK, LGA, EWR)      |
| rate_code_analysis     | Utilisation des codes tarifaires par borough                         |
| trip_duration_analysis | Duree moyenne des trajets et tendances au fil du temps               |

---

## Demarrage rapide

### Prerequis

- Projet Google Cloud avec facturation activee
- APIs activees : BigQuery, Cloud Storage, Cloud Composer
- gcloud CLI configure
- Python 3.10+

### 1. Cloner le depot

```bash
git clone https://github.com/<votre-username>/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline
```

### 2. Installer les dependances

```bash
pip install google-cloud-storage google-cloud-bigquery requests
```

### 3. Configurer le projet GCP

Dans chaque script, mettre a jour la variable :

```python
PROJET_ID = "votre-projet-gcp"
```

### 4. Executer le pipeline manuellement

```bash
# Etape 1 — Extraction et stockage dans GCS
python scripts/extraction_storage.py

# Etape 2 — Chargement dans BigQuery
python scripts/chargement_bigquery.py

# Etape 3 — Transformation et nettoyage
python scripts/transformation.py
```

### 5. Deployer les vues SQL

Executer les fichiers SQL dans la console BigQuery pour creer les 15 vues analytiques :

```
SQL/MarketDemand_and_customer.sql
SQL/Financial_and_Pricing.sql
SQL/CompetitiveInsights.sql
```

### 6. Deployer le DAG Airflow (Cloud Composer)

```bash
gsutil cp chiens/workflow.py gs://<votre-bucket-composer>/dags/
```

---

## Chiffres cles

- Plus de 150 millions de trajets ingerees et traites
- 4 ans de donnees historiques (2021 a aujourd'hui)
- 15 vues analytiques sur 3 domaines metier
- Aucune intervention manuelle — pipeline entierement automatise via Airflow
- Chargement incremental — seuls les nouveaux fichiers sont traites
- Traçabilite complete — tous les logs sont archives dans GCS

---

## Source des donnees

Donnees officielles de la NYC Taxi & Limousine Commission (TLC) :
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## Licence

Ce projet est sous licence MIT — voir le fichier LICENSE pour plus de details.
