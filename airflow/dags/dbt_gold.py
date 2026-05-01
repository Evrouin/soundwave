"""
DAG: dbt_gold
Build gold analytics layer from silver Delta tables.
Produces: dim_track, dim_artist, dim_genre, fact_streams, agg_genre_trends, agg_artist_profiles.
Runs daily after silver transformation completes.
"""
import os
import hashlib

from airflow.decorators import dag, task
from pendulum import datetime

SILVER_TRACKS = "s3://soundwave/silver/tracks"
SILVER_ARTISTS = "s3://soundwave/silver/artists"
GOLD_BASE = "s3://soundwave/gold"

STORAGE_OPTIONS = {
    "AWS_ENDPOINT_URL": "http://minio:9000",
    "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minio"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minio123"),
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}


@dag(
    dag_id="dbt_gold",
    start_date=datetime(2026, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["soundwave", "gold", "analytics"],
    doc_md=__doc__,
)
def dbt_gold():

    @task()
    def build_gold_models():
        """Build all gold dimension and fact tables from silver layer."""
        import pandas as pd
        from deltalake import DeltaTable, write_deltalake

        tracks = DeltaTable(SILVER_TRACKS, storage_options=STORAGE_OPTIONS).to_pandas()
        artists = DeltaTable(SILVER_ARTISTS, storage_options=STORAGE_OPTIONS).to_pandas()
        print(f"Silver tracks: {len(tracks)}, Silver artists: {len(artists)}")

        # ── dim_track ──────────────────────────────────────────
        dim_track = tracks[[
            "track_id", "track_name", "duration_ms",
            "danceability", "energy", "valence", "tempo",
            "acousticness", "instrumentalness",
        ]].copy()
        dim_track["mood_cluster"] = pd.array([None] * len(dim_track), dtype="string")
        write_deltalake(f"{GOLD_BASE}/dim_track", dim_track, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"dim_track: {len(dim_track)} rows")

        # ── dim_artist (SCD Type 2 from silver) ───────────────
        dim_artist = artists.copy()
        dim_artist["artist_sk"] = dim_artist.apply(
            lambda r: hashlib.md5(f"{r['artist_id']}_{r['valid_from']}".encode()).hexdigest(),
            axis=1
        )
        dim_artist = dim_artist[[
            "artist_sk", "artist_id", "artist_name", "artist_genre",
            "artist_popularity", "valid_from", "valid_to", "is_current",
        ]]
        write_deltalake(f"{GOLD_BASE}/dim_artist", dim_artist, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"dim_artist: {len(dim_artist)} rows")

        # ── dim_genre ─────────────────────────────────────────
        genres = tracks["track_genre"].dropna().unique()
        dim_genre = pd.DataFrame({
            "genre_id": [hashlib.md5(g.encode()).hexdigest() for g in genres],
            "genre_name": genres,
        })
        write_deltalake(f"{GOLD_BASE}/dim_genre", dim_genre, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"dim_genre: {len(dim_genre)} rows")

        # ── fact_streams ──────────────────────────────────────
        genre_map = dict(zip(dim_genre["genre_name"], dim_genre["genre_id"]))
        # Map first artist to artist_id
        tracks["_first_artist"] = tracks["artists"].str.split(";").str[0].str.strip()
        artist_id_map = dict(zip(
            artists[artists["is_current"] == True]["artist_name"],
            artists[artists["is_current"] == True]["artist_id"],
        ))

        fact = tracks[["track_id", "popularity", "track_genre", "_first_artist",
                        "ingestion_timestamp"]].copy()
        fact["artist_id"] = fact["_first_artist"].map(artist_id_map)
        fact["genre_id"] = fact["track_genre"].map(genre_map)
        fact["ingestion_date"] = pd.to_datetime(fact["ingestion_timestamp"]).dt.date
        fact["mood_cluster"] = pd.array([None] * len(fact), dtype="string")
        fact["stream_id"] = fact.apply(
            lambda r: hashlib.md5(f"{r['track_id']}_{r['ingestion_date']}".encode()).hexdigest(),
            axis=1
        )
        fact_streams = fact[[
            "stream_id", "track_id", "artist_id", "genre_id",
            "ingestion_date", "popularity", "mood_cluster",
        ]]
        write_deltalake(f"{GOLD_BASE}/fact_streams", fact_streams, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"fact_streams: {len(fact_streams)} rows")

        # ── agg_genre_trends ──────────────────────────────────
        agg_genre = tracks.groupby("track_genre").agg(
            avg_popularity=("popularity", "mean"),
            avg_danceability=("danceability", "mean"),
            avg_energy=("energy", "mean"),
            track_count=("track_id", "count"),
        ).reset_index().rename(columns={"track_genre": "genre_name"})
        write_deltalake(f"{GOLD_BASE}/agg_genre_trends", agg_genre, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"agg_genre_trends: {len(agg_genre)} rows")

        # ── agg_artist_profiles ───────────────────────────────
        tracks["_first_artist_id"] = tracks["_first_artist"].map(artist_id_map)
        agg_artist = tracks.groupby(["_first_artist", "_first_artist_id"]).agg(
            track_count=("track_id", "count"),
            avg_popularity=("popularity", "mean"),
            avg_danceability=("danceability", "mean"),
            avg_energy=("energy", "mean"),
        ).reset_index().rename(columns={
            "_first_artist": "artist_name",
            "_first_artist_id": "artist_id",
        })
        write_deltalake(f"{GOLD_BASE}/agg_artist_profiles", agg_artist, mode="overwrite",
                        storage_options=STORAGE_OPTIONS)
        print(f"agg_artist_profiles: {len(agg_artist)} rows")

    @task()
    def run_gold_tests():
        """Validate gold layer: PK uniqueness, not-null, accepted values."""
        from deltalake import DeltaTable

        errors = []

        def check_pk(path, col, name):
            df = DeltaTable(path, storage_options=STORAGE_OPTIONS).to_pandas()
            nulls = df[col].isna().sum()
            dupes = df[col].duplicated().sum()
            if nulls > 0:
                errors.append(f"{name}.{col}: {nulls} nulls")
            if dupes > 0:
                errors.append(f"{name}.{col}: {dupes} duplicates")
            return df

        check_pk(f"{GOLD_BASE}/dim_track", "track_id", "dim_track")
        check_pk(f"{GOLD_BASE}/dim_artist", "artist_sk", "dim_artist")
        check_pk(f"{GOLD_BASE}/dim_genre", "genre_id", "dim_genre")
        fact = check_pk(f"{GOLD_BASE}/fact_streams", "stream_id", "fact_streams")

        # accepted_values on mood_cluster (allow None until ML phase)
        valid_moods = {None, "energetic", "melancholic", "hype", "chill", "acoustic", "intense"}
        invalid = set(fact["mood_cluster"].dropna().unique()) - valid_moods
        if invalid:
            errors.append(f"fact_streams.mood_cluster invalid values: {invalid}")

        if errors:
            for e in errors:
                print(f"  FAIL: {e}")
            raise ValueError(f"Gold tests failed: {'; '.join(errors)}")
        print("All gold layer tests passed ✓")

    models = build_gold_models()
    run_gold_tests()


dbt_gold()
