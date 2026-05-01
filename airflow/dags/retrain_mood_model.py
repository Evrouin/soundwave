"""
DAG: retrain_mood_model
Weekly mood classification using genre-aware two-tier system.
Tier 1: Genre family (from track_genre). Tier 2: Sub-mood (from audio features within family).
50 sub-moods across 10 genre families.
"""
import os

from airflow.decorators import dag, task
from pendulum import datetime

SILVER_TRACKS = "s3://soundwave/silver/tracks"
GOLD_BASE = "s3://soundwave/gold"
FEAST_REPO = "/opt/airflow/feast"
FEAST_DATA = "/opt/airflow/feast/data"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minio123"),
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}


@dag(
    dag_id="retrain_mood_model",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * 0",
    catchup=False,
    tags=["soundwave", "ml", "retrain"],
    doc_md=__doc__,
)
def retrain_mood_model():

    @task()
    def train_mood_classifier() -> str:
        """Classify tracks into genre family + sub-mood using audio feature scoring."""
        import pandas as pd
        import numpy as np
        import json
        from deltalake import DeltaTable, write_deltalake
        from datetime import datetime as dt, timezone
        from mood_config import GENRE_FAMILY_MAP, GENRE_SUB_MOODS

        os.makedirs(FEAST_DATA, exist_ok=True)

        tracks = DeltaTable(SILVER_TRACKS, storage_options=STORAGE_OPTIONS).to_pandas()
        print(f"Classifying {len(tracks)} tracks")

        # Prepare features
        df = tracks.copy()
        df["loudness_norm"] = (df["loudness"].fillna(-30) + 60) / 60
        df["loudness_norm"] = df["loudness_norm"].clip(0, 1)
        df["mode"] = df["mode"].fillna(0).astype(float)
        df["tempo"] = df["tempo"].fillna(120)

        # Normalize tempo to 0-1 range (50-200 BPM typical range)
        df["tempo_norm"] = ((df["tempo"] - 50) / 150).clip(0, 1)

        # Tier 1: Assign genre family
        df["genre_family"] = df["track_genre"].str.lower().map(GENRE_FAMILY_MAP).fillna("Pop")

        # Tier 2: Score each track against sub-moods within its genre family
        feature_cols = {
            "danceability": "danceability",
            "energy": "energy",
            "valence": "valence",
            "acousticness": "acousticness",
            "instrumentalness": "instrumentalness",
            "tempo": "tempo_norm",
            "loudness_norm": "loudness_norm",
            "speechiness": "speechiness",
            "liveness": "liveness",
            "mode": "mode",
        }

        def score_mood(row, mood_defs):
            """Score a track against mood definitions.
            Positive weight: high feature value = good match.
            Negative weight: low feature value = good match.
            """
            best_mood = "Unknown"
            best_score = -float("inf")
            for mood_name, mood_def in mood_defs.items():
                score = 0.0
                for feat, weight in mood_def["weights"].items():
                    col = feature_cols.get(feat, feat)
                    val = row.get(col, 0.0)
                    if pd.isna(val):
                        val = 0.5
                    if weight < 0:
                        score += abs(weight) * (1 - val)
                    else:
                        score += weight * val
                if score > best_score:
                    best_score = score
                    best_mood = mood_name
            return best_mood

        def classify_track(row):
            family = row["genre_family"]
            sub_moods = GENRE_SUB_MOODS.get(family, {})
            return score_mood(row, sub_moods)

        def classify_mood(row):
            from mood_config import classify_universal_mood
            return classify_universal_mood(
                energy=row.get("energy", 0.5),
                valence=row.get("valence", 0.5),
                danceability=row.get("danceability", 0.5),
                acousticness=row.get("acousticness", 0.0),
                instrumentalness=row.get("instrumentalness", 0.0),
                tempo_norm=row.get("tempo_norm", 0.5),
                loudness_norm=row.get("loudness_norm", 0.5),
                speechiness=row.get("speechiness", 0.0),
                liveness=row.get("liveness", 0.0),
                mode=row.get("mode", 0),
                genre_family=row.get("genre_family", "Pop"),
            )

        df["mood_cluster"] = df.apply(classify_track, axis=1)
        df["mood"] = df.apply(classify_mood, axis=1)

        # Print distribution
        print(f"\nGenre family distribution:")
        print(df["genre_family"].value_counts().to_string())
        print(f"\nTop 20 sub-mood distribution:")
        print(df["mood_cluster"].value_counts().head(20).to_string())

        # Save config to MinIO
        version = dt.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        import s3fs
        fs = s3fs.S3FileSystem(
            endpoint_url="http://minio:9000",
            key=os.environ.get("MINIO_ROOT_USER", "minio"),
            secret=os.environ.get("MINIO_ROOT_PASSWORD", "minio123"),
        )
        config_path = f"/tmp/mood_config_v{version}.json"
        with open(config_path, "w") as f:
            json.dump({"genre_family_map": GENRE_FAMILY_MAP, "sub_moods": GENRE_SUB_MOODS}, f)
        fs.put(config_path, f"soundwave/models/mood/v{version}/config.json")
        print(f"Config saved to s3://soundwave/models/mood/v{version}/")

        # Write Feast offline Parquet
        feast_df = df[["track_id", "mood_cluster", "mood", "genre_family",
                        "danceability", "energy", "valence"]].copy()
        feast_df["event_timestamp"] = pd.Timestamp.now(tz="UTC")
        feast_path = os.path.join(FEAST_DATA, "track_features.parquet")
        feast_df.to_parquet(feast_path, index=False)
        print(f"Feast offline store: {len(feast_df)} rows")

        # Update gold tables
        mood_map = df.set_index("track_id")["mood_cluster"]
        family_map = df.set_index("track_id")["genre_family"]

        dim_track = DeltaTable(f"{GOLD_BASE}/dim_track", storage_options=STORAGE_OPTIONS).to_pandas()
        dim_track["mood_cluster"] = dim_track["track_id"].map(mood_map)
        write_deltalake(f"{GOLD_BASE}/dim_track", dim_track, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)

        fact = DeltaTable(f"{GOLD_BASE}/fact_streams", storage_options=STORAGE_OPTIONS).to_pandas()
        fact["mood_cluster"] = fact["track_id"].map(mood_map)
        write_deltalake(f"{GOLD_BASE}/fact_streams", fact, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print("Gold tables updated")

        return feast_path

    @task()
    def materialize_feast_features(feast_path: str):
        """Materialize features to Redis online store."""
        from feast import FeatureStore
        from datetime import datetime as dt, timezone, timedelta

        store = FeatureStore(repo_path=FEAST_REPO)

        from importlib.util import spec_from_file_location, module_from_spec
        spec = spec_from_file_location("features", os.path.join(FEAST_REPO, "features.py"))
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)

        store.apply([mod.track, mod.track_features])
        now = dt.now(timezone.utc)
        store.materialize(start_date=now - timedelta(days=365), end_date=now)
        print("Features materialized to Redis")

    feast_path = train_mood_classifier()
    materialize_feast_features(feast_path)


retrain_mood_model()
