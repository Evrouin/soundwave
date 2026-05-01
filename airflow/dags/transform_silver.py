"""
DAG: transform_silver
Bronze-to-silver transformation: deduplicate, enforce schema, validate audio feature
ranges, build SCD Type 2 artist dimension, and run data quality checks.
"""
import os

from airflow.decorators import dag, task
from pendulum import datetime

BRONZE_PATH = "s3://soundwave/bronze/tracks"
SILVER_TRACKS_PATH = "s3://soundwave/silver/tracks"
SILVER_ARTISTS_PATH = "s3://soundwave/silver/artists"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minio123"),
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}

RANGE_COLS = ["danceability", "energy", "valence", "acousticness", "instrumentalness",
              "speechiness", "liveness"]


@dag(
    dag_id="transform_silver",
    start_date=datetime(2026, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    tags=["soundwave", "transform", "silver"],
    doc_md=__doc__,
)
def transform_silver():

    @task()
    def run_transform():
        """Deduplicate, enforce schema, validate ranges, write silver tracks + SCD2 artists."""
        import pandas as pd
        import hashlib
        from datetime import datetime as dt, timezone
        from deltalake import DeltaTable, write_deltalake

        # --- Read bronze ---
        bronze = DeltaTable(BRONZE_PATH, storage_options=STORAGE_OPTIONS).to_pandas()
        print(f"Bronze rows: {len(bronze)}")

        # --- Deduplicate on track_id, keep latest ingestion_timestamp ---
        bronze = bronze.sort_values("ingestion_timestamp", ascending=False)
        deduped = bronze.drop_duplicates(subset=["track_id"], keep="first")
        print(f"After dedup: {len(deduped)}")

        # --- Enforce canonical schema ---
        track_cols = {
            "track_id": "str", "track_name": "str", "artists": "str",
            "album_name": "str", "track_genre": "str",
            "popularity": "Int64", "duration_ms": "Int64",
            "explicit": "bool",
            "danceability": "float64", "energy": "float64", "valence": "float64",
            "acousticness": "float64", "instrumentalness": "float64",
            "speechiness": "float64", "liveness": "float64",
            "tempo": "float64", "loudness": "float64",
            "key": "Int64", "mode": "Int64", "time_signature": "Int64",
            "ingestion_timestamp": "datetime64[us, UTC]", "source": "str",
        }
        tracks = deduped[[c for c in track_cols if c in deduped.columns]].copy()
        for col, dtype in track_cols.items():
            if col in tracks.columns and dtype not in ("datetime64[us, UTC]",):
                tracks[col] = tracks[col].astype(dtype, errors="ignore")

        # --- Validate audio feature ranges (0.0-1.0) ---
        for col in RANGE_COLS:
            if col in tracks.columns:
                tracks = tracks[(tracks[col] >= 0.0) & (tracks[col] <= 1.0)]
        tracks = tracks[tracks["tempo"].notna()]
        print(f"After validation: {len(tracks)}")

        # --- Write silver tracks ---
        write_deltalake(SILVER_TRACKS_PATH, tracks, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"Wrote {len(tracks)} rows to silver tracks")

        # --- SCD Type 2 artist dimension ---
        now = dt.now(timezone.utc)
        # Extract unique artists (split semicolon-separated)
        artist_rows = []
        for _, row in tracks[["artists", "track_genre", "popularity"]].drop_duplicates("artists").iterrows():
            for artist_name in str(row["artists"]).split(";"):
                artist_name = artist_name.strip()
                if artist_name:
                    artist_id = hashlib.md5(artist_name.encode()).hexdigest()
                    artist_rows.append({
                        "artist_id": artist_id,
                        "artist_name": artist_name,
                        "artist_genre": row["track_genre"],
                        "artist_popularity": row["popularity"],
                    })
        new_artists = pd.DataFrame(artist_rows).drop_duplicates("artist_id")
        new_artists["row_hash"] = new_artists.apply(
            lambda r: hashlib.md5(f"{r['artist_id']}||{r['artist_name']}||{r['artist_genre']}||{r['artist_popularity']}".encode()).hexdigest(), axis=1
        )

        try:
            existing = DeltaTable(SILVER_ARTISTS_PATH, storage_options=STORAGE_OPTIONS).to_pandas()
            active = existing[existing["is_current"] == True].copy()
            merged = new_artists.merge(active[["artist_id", "row_hash"]], on="artist_id",
                                       how="left", suffixes=("", "_old"))
            changed = merged[(merged["row_hash_old"].isna()) | (merged["row_hash"] != merged["row_hash_old"])]
            changed_ids = set(changed["artist_id"])

            # Expire old rows
            existing.loc[existing["artist_id"].isin(changed_ids) & existing["is_current"], "is_current"] = False
            existing.loc[existing["artist_id"].isin(changed_ids) & ~existing["is_current"], "valid_to"] = now

            # New active rows
            inserts = changed[["artist_id", "artist_name", "artist_genre", "artist_popularity", "row_hash"]].copy()
            inserts["valid_from"] = now
            inserts["valid_to"] = pd.NaT
            inserts["is_current"] = True

            result = pd.concat([existing, inserts], ignore_index=True)
        except Exception:
            # First run — bootstrap
            result = new_artists.copy()
            result["valid_from"] = now
            result["valid_to"] = pd.NaT
            result["is_current"] = True

        write_deltalake(SILVER_ARTISTS_PATH, result, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"Wrote {len(result)} artist dimension rows")

    @task()
    def run_data_quality():
        """Validate silver layer: row counts, nulls, ranges, referential integrity."""
        from deltalake import DeltaTable

        tracks = DeltaTable(SILVER_TRACKS_PATH, storage_options=STORAGE_OPTIONS).to_pandas()
        errors = []

        # FR-DQ-01: Row count minimum
        if len(tracks) < 1000:
            errors.append(f"Row count too low: {len(tracks)}")

        # FR-DQ-01: No nulls on key fields
        for col in ["track_id", "track_name", "artists"]:
            null_count = tracks[col].isna().sum()
            if null_count > 0:
                errors.append(f"Null values in {col}: {null_count}")

        # FR-DQ-01: Audio feature range validity
        for col in RANGE_COLS:
            out_of_range = ((tracks[col] < 0.0) | (tracks[col] > 1.0)).sum()
            if out_of_range > 0:
                errors.append(f"{col} has {out_of_range} values outside [0,1]")

        # FR-DQ-01: Referential integrity — all artist names in tracks exist in artists
        artists = DeltaTable(SILVER_ARTISTS_PATH, storage_options=STORAGE_OPTIONS).to_pandas()
        artist_names = set(artists["artist_name"])
        track_artists = set()
        for a in tracks["artists"].dropna():
            for name in str(a).split(";"):
                track_artists.add(name.strip())
        missing = track_artists - artist_names
        if missing:
            errors.append(f"{len(missing)} artists in tracks not found in artist dimension")

        # Report
        print(f"Data quality checks: {len(errors)} issues found")
        for e in errors:
            print(f"  FAIL: {e}")

        if errors:
            raise ValueError(f"Data quality failed: {'; '.join(errors)}")
        print("All data quality checks passed ✓")

    transform_result = run_transform()
    run_data_quality()


transform_silver()
