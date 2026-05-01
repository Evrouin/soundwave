"""Silver layer transformation: dedup, schema enforcement, validation, and SCD2 artists."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from soundwave.config.paths import Paths
from soundwave.config.storage import StorageConfig

RANGE_COLS = [
    "danceability", "energy", "valence", "acousticness",
    "instrumentalness", "speechiness", "liveness",
]

TRACK_SCHEMA: dict[str, str] = {
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


class SilverTransformer:
    """Transforms bronze data into the curated silver layer."""

    def __init__(self, storage_config: StorageConfig) -> None:
        """Initialise with storage configuration.

        Args:
            storage_config: MinIO/S3 credentials and endpoint.
        """
        self.storage = storage_config

    def transform(self) -> pd.DataFrame:
        """Read bronze tracks, deduplicate, enforce schema, validate, and write silver.

        Returns:
            The cleaned silver tracks DataFrame.
        """
        opts = self.storage.to_dict()
        bronze = DeltaTable(Paths.BRONZE_TRACKS, storage_options=opts).to_pandas()

        bronze = bronze.sort_values("ingestion_timestamp", ascending=False)
        tracks = bronze.drop_duplicates(subset=["track_id"], keep="first")

        present = [c for c in TRACK_SCHEMA if c in tracks.columns]
        tracks = tracks[present].copy()
        for col, dtype in TRACK_SCHEMA.items():
            if col in tracks.columns and dtype != "datetime64[us, UTC]":
                tracks[col] = tracks[col].astype(dtype, errors="ignore")

        for col in RANGE_COLS:
            if col in tracks.columns:
                tracks = tracks[(tracks[col] >= 0.0) & (tracks[col] <= 1.0)]
        tracks = tracks[tracks["tempo"].notna()]

        write_deltalake(
            Paths.SILVER_TRACKS, tracks, mode="overwrite", storage_options=opts,
        )
        return tracks

    def build_artist_dimension(self) -> pd.DataFrame:
        """Extract unique artists from silver tracks and write an SCD Type 2 dimension.

        Returns:
            The artist dimension DataFrame.
        """
        opts = self.storage.to_dict()
        tracks = DeltaTable(Paths.SILVER_TRACKS, storage_options=opts).to_pandas()
        now = datetime.now(timezone.utc)

        rows: list[dict] = []
        seen: set[str] = set()
        for _, row in tracks.iterrows():
            for name in str(row["artists"]).split(";"):
                name = name.strip()
                if not name:
                    continue
                aid = hashlib.md5(name.encode()).hexdigest()
                if aid in seen:
                    continue
                seen.add(aid)
                rows.append({
                    "artist_id": aid,
                    "artist_name": name,
                    "artist_genre": row["track_genre"],
                    "artist_popularity": row["popularity"],
                })

        new = pd.DataFrame(rows)
        new["row_hash"] = new.apply(
            lambda r: hashlib.md5(
                f"{r['artist_id']}||{r['artist_name']}||{r['artist_genre']}||{r['artist_popularity']}".encode()
            ).hexdigest(),
            axis=1,
        )

        try:
            existing = DeltaTable(Paths.SILVER_ARTISTS, storage_options=opts).to_pandas()
            active = existing[existing["is_current"]].copy()
            merged = new.merge(
                active[["artist_id", "row_hash"]],
                on="artist_id", how="left", suffixes=("", "_old"),
            )
            changed = merged[
                merged["row_hash_old"].isna() | (merged["row_hash"] != merged["row_hash_old"])
            ]
            changed_ids = set(changed["artist_id"])

            existing.loc[
                existing["artist_id"].isin(changed_ids) & existing["is_current"],
                "is_current",
            ] = False
            existing.loc[
                existing["artist_id"].isin(changed_ids) & ~existing["is_current"],
                "valid_to",
            ] = now

            inserts = changed[
                ["artist_id", "artist_name", "artist_genre", "artist_popularity", "row_hash"]
            ].copy()
            inserts["valid_from"] = now
            inserts["valid_to"] = pd.Series([pd.NaT] * len(inserts), dtype="datetime64[us, UTC]")
            inserts["is_current"] = True

            result = pd.concat([existing, inserts], ignore_index=True)
        except Exception:
            result = new.copy()
            result["valid_from"] = now
            result["valid_to"] = pd.Series([pd.NaT] * len(new), dtype="datetime64[us, UTC]")
            result["is_current"] = True

        result["valid_from"] = pd.to_datetime(result["valid_from"], utc=True)
        result["valid_to"] = pd.to_datetime(result["valid_to"], utc=True)
        write_deltalake(
            Paths.SILVER_ARTISTS, result, mode="overwrite", storage_options=opts,
        )
        return result

    def validate(self) -> None:
        """Run data quality checks on the silver layer.

        Raises:
            ValueError: If any quality check fails.
        """
        opts = self.storage.to_dict()
        tracks = DeltaTable(Paths.SILVER_TRACKS, storage_options=opts).to_pandas()
        artists = DeltaTable(Paths.SILVER_ARTISTS, storage_options=opts).to_pandas()
        errors: list[str] = []

        if len(tracks) < 1000:
            errors.append(f"Row count too low: {len(tracks)}")

        for col in ("track_id", "track_name", "artists"):
            nulls = tracks[col].isna().sum()
            if nulls > 0:
                errors.append(f"Null values in {col}: {nulls}")

        for col in RANGE_COLS:
            bad = ((tracks[col] < 0.0) | (tracks[col] > 1.0)).sum()
            if bad > 0:
                errors.append(f"{col} has {bad} values outside [0,1]")

        artist_names = set(artists["artist_name"])
        track_artists: set[str] = set()
        for val in tracks["artists"].dropna():
            for name in str(val).split(";"):
                track_artists.add(name.strip())
        missing = track_artists - artist_names
        if missing:
            errors.append(f"{len(missing)} artists in tracks not found in artist dimension")

        if errors:
            raise ValueError(f"Data quality failed: {'; '.join(errors)}")
