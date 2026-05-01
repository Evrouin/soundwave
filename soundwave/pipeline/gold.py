"""Gold layer builder for the Soundwave medallion architecture."""

import hashlib

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from soundwave.config.paths import Paths
from soundwave.config.storage import StorageConfig


class GoldBuilder:
    """Build gold dimension, fact, and aggregate tables from silver Delta tables."""

    def __init__(self, storage_config: StorageConfig) -> None:
        """Initialize GoldBuilder with storage configuration.

        Args:
            storage_config: StorageConfig instance for Delta Lake access.
        """
        self._storage = storage_config
        self._opts = storage_config.to_dict()

    def build(self) -> dict[str, int]:
        """Read silver tracks and artists, build all 6 gold tables.

        Returns:
            Dictionary mapping table name to row count.
        """
        tracks = DeltaTable(Paths.SILVER_TRACKS, storage_options=self._opts).to_pandas()
        artists = DeltaTable(Paths.SILVER_ARTISTS, storage_options=self._opts).to_pandas()

        counts = {}
        counts["dim_track"] = self._build_dim_track(tracks)
        counts["dim_artist"] = self._build_dim_artist(artists)
        counts["dim_genre"] = self._build_dim_genre(tracks)
        counts["fact_streams"] = self._build_fact_streams(tracks, artists)
        counts["agg_genre_trends"] = self._build_agg_genre_trends(tracks)
        counts["agg_artist_profiles"] = self._build_agg_artist_profiles(tracks, artists)
        return counts

    def validate(self) -> None:
        """Check PK uniqueness and not-null constraints on gold tables.

        Raises:
            ValueError: If any primary key constraint is violated.
        """
        checks = {
            "dim_track": "track_id",
            "dim_artist": "artist_sk",
            "dim_genre": "genre_id",
            "fact_streams": "stream_id",
        }
        errors = []
        for table, pk in checks.items():
            df = DeltaTable(
                f"{Paths.GOLD_BASE}/{table}", storage_options=self._opts
            ).to_pandas()
            nulls = df[pk].isna().sum()
            dupes = df[pk].duplicated().sum()
            if nulls > 0:
                errors.append(f"{table}.{pk}: {nulls} nulls")
            if dupes > 0:
                errors.append(f"{table}.{pk}: {dupes} duplicates")
        if errors:
            raise ValueError(f"Gold validation failed: {'; '.join(errors)}")

    def _write(self, table_name: str, df: pd.DataFrame) -> int:
        """Write a DataFrame as a Delta table under GOLD_BASE."""
        write_deltalake(
            f"{Paths.GOLD_BASE}/{table_name}",
            df,
            mode="overwrite",
            storage_options=self._opts,
        )
        return len(df)

    def _build_dim_track(self, tracks: pd.DataFrame) -> int:
        dim_track = tracks[[
            "track_id", "track_name", "duration_ms",
            "danceability", "energy", "valence", "tempo",
            "acousticness", "instrumentalness",
        ]].copy()
        dim_track["mood_cluster"] = pd.array([None] * len(dim_track), dtype="string")
        return self._write("dim_track", dim_track)

    def _build_dim_artist(self, artists: pd.DataFrame) -> int:
        dim_artist = artists.copy()
        dim_artist["artist_sk"] = dim_artist.apply(
            lambda r: hashlib.md5(
                f"{r['artist_id']}_{r['valid_from']}".encode()
            ).hexdigest(),
            axis=1,
        )
        dim_artist = dim_artist[[
            "artist_sk", "artist_id", "artist_name", "artist_genre",
            "artist_popularity", "valid_from", "valid_to", "is_current",
        ]]
        return self._write("dim_artist", dim_artist)

    def _build_dim_genre(self, tracks: pd.DataFrame) -> int:
        genres = tracks["track_genre"].dropna().unique()
        dim_genre = pd.DataFrame({
            "genre_id": [hashlib.md5(g.encode()).hexdigest() for g in genres],
            "genre_name": genres,
        })
        return self._write("dim_genre", dim_genre)

    def _build_fact_streams(
        self, tracks: pd.DataFrame, artists: pd.DataFrame
    ) -> int:
        genre_dt = DeltaTable(
            f"{Paths.GOLD_BASE}/dim_genre", storage_options=self._opts
        ).to_pandas()
        genre_map = dict(zip(genre_dt["genre_name"], genre_dt["genre_id"]))

        current = artists[artists["is_current"] == True]
        artist_id_map = dict(zip(current["artist_name"], current["artist_id"]))

        fact = tracks[["track_id", "popularity", "track_genre", "artists",
                        "ingestion_timestamp"]].copy()
        fact["_first_artist"] = fact["artists"].str.split(";").str[0].str.strip()
        fact["artist_id"] = fact["_first_artist"].map(artist_id_map)
        fact["genre_id"] = fact["track_genre"].map(genre_map)
        fact["ingestion_date"] = pd.to_datetime(fact["ingestion_timestamp"]).dt.date
        fact["mood_cluster"] = pd.array([None] * len(fact), dtype="string")
        fact["stream_id"] = fact.apply(
            lambda r: hashlib.md5(
                f"{r['track_id']}_{r['ingestion_date']}".encode()
            ).hexdigest(),
            axis=1,
        )
        fact_streams = fact[[
            "stream_id", "track_id", "artist_id", "genre_id",
            "ingestion_date", "popularity", "mood_cluster",
        ]]
        return self._write("fact_streams", fact_streams)

    def _build_agg_genre_trends(self, tracks: pd.DataFrame) -> int:
        agg = tracks.groupby("track_genre").agg(
            avg_popularity=("popularity", "mean"),
            avg_danceability=("danceability", "mean"),
            avg_energy=("energy", "mean"),
            track_count=("track_id", "count"),
        ).reset_index().rename(columns={"track_genre": "genre_name"})
        return self._write("agg_genre_trends", agg)

    def _build_agg_artist_profiles(
        self, tracks: pd.DataFrame, artists: pd.DataFrame
    ) -> int:
        current = artists[artists["is_current"] == True]
        artist_id_map = dict(zip(current["artist_name"], current["artist_id"]))

        df = tracks.copy()
        df["_first_artist"] = df["artists"].str.split(";").str[0].str.strip()
        df["_first_artist_id"] = df["_first_artist"].map(artist_id_map)
        agg = df.groupby(["_first_artist", "_first_artist_id"]).agg(
            track_count=("track_id", "count"),
            avg_popularity=("popularity", "mean"),
            avg_danceability=("danceability", "mean"),
            avg_energy=("energy", "mean"),
        ).reset_index().rename(columns={
            "_first_artist": "artist_name",
            "_first_artist_id": "artist_id",
        })
        return self._write("agg_artist_profiles", agg)
