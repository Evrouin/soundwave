# Soundwave

End-to-end music analytics and mood-recommendation platform powered by Spotify data, Delta Lake, and ML.

Ingests ~114K Spotify tracks from the [Spotify Tracks Dataset](https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset) by Maharshi Pandya, transforms them through a Medallion architecture (Bronze → Silver → Gold), classifies every track into one of 10 universal moods and 50 genre-specific sub-labels, and serves results through a REST API and BI dashboard.

## Architecture

```
Kaggle CSV  ──────────────────────────────────────────┐
                                                       ▼
Spotify API  ──── Airflow DAG ──── Bronze (Delta) ──── pandas ──── Silver (Delta)
                                                                          │
                                                                   Gold (Delta)
                                                                   /         \
                                                             Trino            Mood Classifier
                                                               │              (rule-based + genre bias)
                                                          Metabase                  │
                                                         Dashboard            Feast store
                                                                              │       │
                                                                         Parquet   Redis
                                                                                       │
                                                                                   FastAPI
                                                                                /mood/{id}
```

## Quick Start

```bash
# 1. Clone and configure
git clone https://github.com/yourusername/soundwave.git
cd soundwave
cp .env.example .env
# Fill in SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, KAGGLE_USERNAME, KAGGLE_KEY

# 2. Start the stack
docker compose build
docker compose up -d

# 3. Run the pipeline (once all services are healthy)
docker compose exec airflow-api-server airflow dags trigger ingest_kaggle
# Wait for completion, then:
docker compose exec airflow-scheduler airflow dags test transform_silver
docker compose exec airflow-scheduler airflow dags test dbt_gold
docker compose exec airflow-scheduler airflow dags test retrain_mood_model

# 4. Query the API
curl http://localhost:8085/mood/5SuOikwiRyPMVoIQDJUgSV
```

## Prerequisites

- Docker & Docker Compose
- Spotify Developer account (client ID & secret)
- Kaggle account (API token)

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master | http://localhost:8081 | — |
| Trino | http://localhost:8083 | — |
| MinIO Console | http://localhost:9011 | minio / minio123 |
| Metabase | http://localhost:3000 | (setup on first visit) |
| Mood API | http://localhost:8085 | — |

## Project Structure

```
soundwave/
├── .github/workflows/
│   └── ci.yml                          # GitHub Actions: pytest, ruff, docker validate
├── airflow/dags/
│   ├── ingest_kaggle.py                # Kaggle CSV → Bronze Delta (manual trigger)
│   ├── ingest_spotify.py               # Spotify API → Bronze Delta (daily 06:00 UTC)
│   ├── transform_silver.py             # Bronze → Silver (dedup, schema, DQ checks)
│   ├── dbt_gold.py                     # Silver → Gold (6 tables + PK/null tests)
│   ├── retrain_mood_model.py           # Mood classification + Feast materialization
│   └── mood_config.py                  # 114 genres → 10 families → 50 sub-labels + 10 moods
├── api/
│   ├── main.py                         # FastAPI: /mood/{track_id}, /health
│   └── requirements.txt
├── feast/
│   ├── feature_store.yaml              # Parquet offline + Redis online store
│   └── features.py                     # Track entity + feature view
├── dbt/
│   ├── dbt_project.yml
│   └── models/marts/                   # dim_track, dim_artist, dim_genre, fact_streams, agg_*
├── spark/
│   ├── bronze_to_silver.py             # PySpark ETL (reference implementation)
│   └── train_kmeans.py                 # PySpark MLlib K-Means (reference implementation)
├── great_expectations/
│   └── checkpoints/silver_checkpoint.yml
├── trino/catalog/
│   └── delta.properties                # Delta Lake connector → MinIO
├── tests/
│   ├── test_api.py                     # API endpoint tests
│   └── test_mood.py                    # Mood classification + config integrity tests
├── docs/
│   ├── business-requirements.md        # Full requirements spec
│   └── mood_clusters.md                # Mood cluster documentation
├── docker-compose.yml                  # Full stack (13 services)
├── Dockerfile.airflow                  # Custom Airflow image with deps
├── Dockerfile.api                      # FastAPI image
├── requirements.txt                    # Python dependencies (pinned)
├── .env.example                        # Credential template
└── .python-version                     # Python 3.12.11 (pyenv)
```

## Data Pipeline

### Medallion Architecture

| Layer | Storage | Contents |
|-------|---------|----------|
| **Bronze** | `s3://soundwave/bronze/tracks` | Raw Kaggle CSV + ingestion metadata |
| **Silver** | `s3://soundwave/silver/tracks` | Deduplicated (89,741 tracks), schema-enforced, range-validated |
| | `s3://soundwave/silver/artists` | SCD Type 2 artist dimension (29,859 rows) |
| **Gold** | `s3://soundwave/gold/dim_track` | Track dimension with mood clusters |
| | `s3://soundwave/gold/dim_artist` | Artist dimension (SCD2 with surrogate keys) |
| | `s3://soundwave/gold/dim_genre` | 114 genre dimension |
| | `s3://soundwave/gold/fact_streams` | Fact table with mood + popularity |
| | `s3://soundwave/gold/agg_genre_trends` | Genre-level aggregations |
| | `s3://soundwave/gold/agg_artist_profiles` | Artist-level aggregations |

### Airflow DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `ingest_kaggle` | Manual | One-time bulk load of Kaggle dataset |
| `ingest_spotify` | Daily 06:00 UTC | Incremental Spotify API ingestion |
| `transform_silver` | Daily 07:00 UTC | Bronze → Silver + data quality checks |
| `dbt_gold` | Daily 08:00 UTC | Silver → Gold models + PK/null tests |
| `retrain_mood_model` | Weekly (Sunday) | Mood classification + Feast materialization |

## Mood Classification

### Two-Tier System

**Tier 1 — Universal Mood** (10 labels, genre-biased rule-based):

| Mood | Description | Key Signals |
|------|-------------|-------------|
| Euphoric | Joyful, uplifting, peak happiness | High energy + high valence + high danceability |
| Energetic | High-energy, pumped, and powerful | High energy + high tempo |
| Groovy | Rhythmic, funky, and head-nodding | High danceability + positive valence |
| Relaxed | Calm, feel-good, and easy-going | Low energy + positive valence |
| Dreamy | Atmospheric, floating, and ethereal | High instrumentalness/acousticness + low energy |
| Romantic | Warm, intimate, and tender | Low energy + acoustic + moderate valence |
| Uplifting | Hopeful, inspiring, and triumphant | High valence + major key |
| Melancholic | Sad, reflective, and bittersweet | Low valence + low energy |
| Dark | Brooding, moody, and ominous | Low valence + moderate energy + minor key |
| Aggressive | Intense, angry, and raw | High energy + low valence |

Genre family bias nudges borderline tracks (e.g., Jazz/Soul → Groovy, Metal → Aggressive).

**Tier 2 — Genre Sub-Label** (50 labels across 10 genre families):

10 genre families (Pop, Hip-Hop/Rap, Electronic/EDM, Rock/Alternative, Jazz/Soul, Classical/Ambient, Latin/World, Metal/Hardcore, Folk/Country, R&B/Funk) × 5 sub-labels each, scored by weighted audio features.

### API Response

```json
{
  "track_id": "5SuOikwiRyPMVoIQDJUgSV",
  "genre_family": "Pop",
  "mood_label": "Chill Pop",
  "mood_description": "Relaxed, feel-good, and easy-going",
  "danceability": 0.676,
  "energy": 0.461,
  "valence": 0.715
}
```

## Tech Stack

| Layer | Tool |
|-------|------|
| Ingestion | Airflow 3.2, Spotify API, Kaggle API |
| Storage | MinIO (S3-compatible), Delta Lake |
| Processing | pandas, deltalake |
| Modeling | dbt-core, dbt-trino |
| ML / Features | scikit-learn, Feast, Redis |
| Serving | FastAPI, Trino |
| Visualization | Metabase |
| Quality | Great Expectations, pytest |
| CI | GitHub Actions |

## Running Tests

```bash
# Activate venv
source .venv/bin/activate

# Run all tests
pytest tests/ -v

# Lint
pip install ruff
ruff check airflow/dags/ api/ spark/
```

## License

MIT
