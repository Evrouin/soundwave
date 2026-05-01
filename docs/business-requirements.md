# Business requirements — Soundwave analytics platform

## 1. Executive summary

Soundwave is an end-to-end data engineering portfolio project that demonstrates production-grade lakehouse architecture applied to Spotify music data. The platform ingests batch and live data from two sources, transforms it through a Medallion architecture, enriches it with ML-derived mood classifications, and serves insights through a live BI dashboard and a REST API.

The primary goal is to showcase the full data engineering lifecycle — from raw ingestion to stakeholder-facing output — using an industry-standard open-source stack.

---

## 2. Business objectives

| ID | Objective |
|----|-----------|
| BO-01 | Ingest and unify Spotify track data from a bulk historical dataset and a live API source into a single reliable data store. |
| BO-02 | Produce clean, tested, and documented gold-layer datasets that answer genre trend and audio-feature questions. |
| BO-03 | Classify every track into a mood archetype using unsupervised ML, and serve those classifications at low latency via a REST API. |
| BO-04 | Deliver a live BI dashboard surfacing genre trends, top artists, and mood distributions, refreshed daily. |
| BO-05 | Maintain full pipeline observability with data quality checks, alerting, and lineage documentation. |
| BO-06 | Deploy the entire stack via a single `docker-compose up` command to enable reproducibility and easy demonstration. |

---

## 3. Scope

### In scope

- Batch ingestion of the Kaggle Spotify Tracks dataset (~600K rows).
- Daily incremental ingestion from the Spotify Web API (new releases, chart data).
- Bronze → Silver → Gold Medallion transformation pipeline.
- SCD Type 2 slowly-changing dimension for artist metadata.
- K-Means mood clustering model trained on audio features.
- Feast feature store with offline (Parquet) and online (Redis) materialization.
- FastAPI REST endpoint serving mood classifications by track ID.
- Metabase dashboard with at least five live charts.
- Automated data quality suite using dbt tests and Great Expectations.
- CI pipeline via GitHub Actions running tests on every push.
- Full infrastructure defined in Docker Compose for local deployment.

### Out of scope

- User authentication or multi-tenancy on the API.
- Real-time streaming ingestion (Kafka, Flink) — daily batch cadence is sufficient.
- Lyrics analysis or NLP.
- Mobile or web front-end beyond the Metabase dashboard.
- Production cloud deployment (Terraform + AWS/GCP is optional stretch goal).

---

## 4. Data sources

### 4.1 Kaggle — Spotify Tracks dataset

| Attribute | Detail |
|-----------|--------|
| Source | Kaggle — "Spotify Tracks Dataset" by Maharshi Pandya |
| Format | CSV |
| Volume | ~600,000 rows |
| Key fields | `track_id`, `track_name`, `artists`, `album_name`, `track_genre`, `popularity`, `duration_ms`, `danceability`, `energy`, `loudness`, `speechiness`, `acousticness`, `instrumentalness`, `liveness`, `valence`, `tempo`, `time_signature` |
| Ingestion type | One-time bulk load on first run; schema versioned thereafter |
| Access | Kaggle API token (`kaggle.json`) |

### 4.2 Spotify Web API

| Attribute | Detail |
|-----------|--------|
| Source | Spotify for Developers — Web API |
| Endpoints used | `/browse/new-releases`, `/browse/featured-playlists`, `/tracks`, `/audio-features` |
| Rate limit | ~180 requests per minute |
| Auth | OAuth 2.0 Client Credentials flow |
| Ingestion type | Daily incremental — new releases and chart updates only |
| Volume | ~500–2,000 rows per daily run |

---

## 5. Functional requirements

### 5.1 Ingestion

| ID | Requirement |
|----|-------------|
| FR-ING-01 | The system must download the Kaggle dataset on first run and store it in the bronze Delta Lake table. |
| FR-ING-02 | The system must authenticate with the Spotify Web API using Client Credentials and handle token refresh automatically. |
| FR-ING-03 | The Airflow DAG must run daily at 06:00 UTC, pulling new releases and chart data from the Spotify API. |
| FR-ING-04 | All raw data must land in the bronze layer unmodified, with an ingestion timestamp and source identifier added as metadata columns. |
| FR-ING-05 | The ingestion job must log the row count of each load and alert (email or Slack) if the count drops below a configurable threshold. |
| FR-ING-06 | The system must handle Spotify API rate limiting gracefully with exponential backoff and retry logic. |

### 5.2 Transformation

| ID | Requirement |
|----|-------------|
| FR-TRF-01 | A PySpark job must deduplicate bronze records on `track_id`, retaining the most recent ingestion timestamp. |
| FR-TRF-02 | The silver layer must enforce a canonical schema: consistent data types, null handling, and field naming conventions (snake_case). |
| FR-TRF-03 | Audio feature fields (`danceability`, `energy`, `valence`, `tempo`, etc.) must be validated as floats within their documented Spotify ranges. |
| FR-TRF-04 | The `artist` dimension must implement SCD Type 2 — tracking changes to `artist_genre` and `artist_popularity` over time with `valid_from`, `valid_to`, and `is_current` columns. |
| FR-TRF-05 | dbt must produce the following gold models: `dim_artist`, `dim_genre`, `dim_track`, `fact_streams`, `agg_genre_trends`, `agg_artist_profiles`. |
| FR-TRF-06 | All dbt models must have at minimum: `not_null` and `unique` tests on primary keys, and `accepted_values` tests on categorical fields. |
| FR-TRF-07 | dbt model documentation must be generated and committed (`dbt docs generate`) for every model. |

### 5.3 ML enrichment

| ID | Requirement |
|----|-------------|
| FR-ML-01 | A K-Means model must be trained using PySpark MLlib on the following features: `danceability`, `energy`, `valence`, `acousticness`, `instrumentalness`, `tempo` (normalized). |
| FR-ML-02 | The model must produce exactly 6 mood clusters, labelled: `energetic`, `melancholic`, `hype`, `chill`, `acoustic`, `intense`. |
| FR-ML-03 | Cluster labels must be assigned based on centroid analysis and documented in `docs/mood_clusters.md`. |
| FR-ML-04 | The trained model must be versioned and stored in MinIO under `models/kmeans/v{version}/`. |
| FR-ML-05 | A Feast feature store must register `mood_cluster`, `danceability`, `energy`, and `valence` as features on the `track` entity. |
| FR-ML-06 | The Airflow DAG must include a weekly retrain step that retrains the model on the latest silver data and materializes updated features to the online store (Redis). |
| FR-ML-07 | Feature materialization must complete within 30 minutes of the weekly retrain. |

### 5.4 Serving

| ID | Requirement |
|----|-------------|
| FR-SRV-01 | A FastAPI application must expose a `GET /mood/{track_id}` endpoint returning `{ track_id, mood_label, danceability, energy, valence }`. |
| FR-SRV-02 | The API must return a `404` with a descriptive message for unknown track IDs. |
| FR-SRV-03 | The API must serve responses from the Redis online feature store with p99 latency under 50ms. |
| FR-SRV-04 | The API must expose a `/health` endpoint returning `{ status: "ok", timestamp }`. |
| FR-SRV-05 | Trino must be configured as the query engine over the gold Delta Lake tables, accessible to Metabase. |
| FR-SRV-06 | The Metabase dashboard must include the following charts: (1) top 10 genres by average popularity, (2) danceability vs energy scatter by genre, (3) mood distribution pie chart, (4) new releases trend over time, (5) top artists by track count. |
| FR-SRV-07 | The Metabase dashboard must refresh automatically on a daily schedule aligned with the Airflow DAG completion. |

### 5.5 Data quality

| ID | Requirement |
|----|-------------|
| FR-DQ-01 | A Great Expectations suite must validate the silver layer after each Spark job, checking: row count minimums, no nulls on key fields, audio feature range validity, and referential integrity between track and artist. |
| FR-DQ-02 | Validation failures must fail the Airflow DAG task and trigger an alert. |
| FR-DQ-03 | A data quality report must be generated as an HTML artifact and stored in MinIO after each run. |

---

## 6. Non-functional requirements

| ID | Category | Requirement |
|----|----------|-------------|
| NFR-01 | Reproducibility | The full stack must start with `docker-compose up` from a clean clone with no manual configuration beyond supplying API credentials in a `.env` file. |
| NFR-02 | Performance | The daily Airflow DAG must complete end-to-end within 60 minutes on a machine with 16GB RAM. |
| NFR-03 | Scalability | The PySpark jobs must be written to handle 10× the current data volume (6M rows) without code changes — only Spark config tuning. |
| NFR-04 | Observability | All Airflow tasks must emit structured logs. DAG run durations and task success rates must be visible in the Airflow UI. |
| NFR-05 | Portability | No step in the pipeline may depend on a local file path outside the Docker volume mounts defined in `docker-compose.yml`. |
| NFR-06 | Security | API credentials (Spotify client ID/secret, Kaggle token) must be loaded from environment variables only. No secrets committed to Git. |
| NFR-07 | CI | GitHub Actions must run `dbt test`, `great_expectations checkpoint run`, and `pytest` on every pull request to `main`. |
| NFR-08 | Documentation | Every dbt model, every Airflow DAG, and every FastAPI endpoint must have a docstring or description field. |

---

## 7. Data model — gold layer

### fact_streams

| Column | Type | Description |
|--------|------|-------------|
| stream_id | STRING (PK) | Surrogate key |
| track_id | STRING (FK) | References dim_track |
| artist_id | STRING (FK) | References dim_artist |
| genre_id | STRING (FK) | References dim_genre |
| ingestion_date | DATE | Date the record was ingested |
| popularity | INTEGER | Spotify popularity score (0–100) |
| mood_cluster | STRING | ML-derived mood label |

### dim_track

| Column | Type | Description |
|--------|------|-------------|
| track_id | STRING (PK) | Spotify track URI |
| track_name | STRING | Track title |
| duration_ms | INTEGER | Duration in milliseconds |
| danceability | FLOAT | 0.0–1.0 |
| energy | FLOAT | 0.0–1.0 |
| valence | FLOAT | 0.0–1.0 |
| tempo | FLOAT | BPM |
| acousticness | FLOAT | 0.0–1.0 |
| instrumentalness | FLOAT | 0.0–1.0 |
| mood_cluster | STRING | From Feast feature store |

### dim_artist (SCD Type 2)

| Column | Type | Description |
|--------|------|-------------|
| artist_sk | STRING (PK) | Surrogate key |
| artist_id | STRING | Spotify artist URI |
| artist_name | STRING | Display name |
| artist_genre | STRING | Primary genre |
| artist_popularity | INTEGER | Popularity at snapshot time |
| valid_from | DATE | Start of validity window |
| valid_to | DATE | End of validity window (null = current) |
| is_current | BOOLEAN | True for the active record |

---

## 8. Pipeline architecture

```
Kaggle CSV  ──────────────────────────────────────────┐
                                                       ▼
Spotify API  ──── Airflow DAG ──── Bronze (Delta) ──── PySpark ──── Silver (Delta)
                                                                          │
                                                                     dbt models
                                                                          │
                                                                     Gold (Delta)
                                                                     /         \
                                                               Trino            PySpark MLlib
                                                                 │                    │
                                                            Metabase             Feast store
                                                           Dashboard              │       │
                                                                             Parquet   Redis
                                                                                         │
                                                                                     FastAPI
                                                                                  /mood/{id}
```

---

## 9. Repo structure

```
soundwave/
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions — dbt test, pytest, GE
├── airflow/
│   └── dags/
│       ├── ingest_kaggle.py
│       ├── ingest_spotify.py
│       ├── transform_silver.py
│       ├── dbt_gold.py
│       └── retrain_mood_model.py
├── spark/
│   ├── bronze_to_silver.py
│   └── train_kmeans.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── tests/
│   └── dbt_project.yml
├── feast/
│   ├── feature_store.yaml
│   └── features.py
├── api/
│   ├── main.py
│   └── requirements.txt
├── great_expectations/
│   └── checkpoints/
├── docs/
│   └── mood_clusters.md
├── docker-compose.yml
├── .env.example
├── README.md
└── REQUIREMENTS.md
```

---

## 10. Milestones

| Phase | Deliverable | Suggested timeline |
|-------|-------------|-------------------|
| 1 | Docker Compose stack running (Airflow, Spark, MinIO, Redis, Trino, Metabase) | Week 1 |
| 2 | Kaggle bulk ingestion → bronze Delta table | Week 1 |
| 3 | Spotify API daily DAG → bronze incremental | Week 2 |
| 4 | PySpark silver transform + Great Expectations suite | Week 2 |
| 5 | dbt gold models + SCD Type 2 artist dimension | Week 3 |
| 6 | K-Means mood clustering + Feast feature store | Week 3–4 |
| 7 | FastAPI `/mood/{track_id}` endpoint | Week 4 |
| 8 | Metabase dashboard (5 charts) | Week 4 |
| 9 | GitHub Actions CI pipeline | Week 5 |
| 10 | README, architecture diagram, Loom demo recording | Week 5 |

---

## 11. Future phases

### Phase 2 — Nuxt 4 frontend (planned, not in scope for v1)

A consumer-facing web application will be built in a future phase on top of the existing FastAPI layer. Planned pages:

- **Track mood lookup** — search by track name, display mood label and audio feature bars, powered by `GET /mood/{track_id}`.
- **Genre trend explorer** — SSR-rendered line charts of danceability/energy over time by genre, queried from the gold layer via a thin API route.
- **Mood cluster scatter** — client-side PCA scatter plot of tracks colored by mood cluster, explaining the ML layer visually.

The Nuxt app will be deployed to Vercel (free tier). The FastAPI backend will remain fully decoupled — the frontend consumes only the public REST API with no direct database access.

This phase does not change any DE requirements. The FastAPI contract defined in FR-SRV-01 through FR-SRV-04 is the stable interface both phases share.

---

## 12. Success criteria

The project is considered complete when all of the following are true:

- `docker-compose up` starts the full stack with no errors on a clean machine.
- The Airflow DAG runs end-to-end without failures on sample data.
- All dbt tests pass on the gold layer.
- The Great Expectations checkpoint passes on the silver layer.
- `GET /mood/{track_id}` returns a correct response for any track in the dataset within 50ms.
- The Metabase dashboard displays all five charts with data.
- GitHub Actions CI passes on the `main` branch.
- `README.md` includes an architecture diagram, setup instructions, and a link to a demo recording or live dashboard.