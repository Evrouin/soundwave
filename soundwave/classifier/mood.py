"""Two-tier mood classifier: universal mood + genre-specific sub-label."""

import os

import pandas as pd
from deltalake import DeltaTable, write_deltalake

from soundwave.config.mood_config import GENRE_FAMILY_MAP, GENRE_SUB_MOODS, classify_universal_mood
from soundwave.config.paths import Paths
from soundwave.config.storage import StorageConfig

FEATURE_COLS = {
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


def _score_mood(row: pd.Series, mood_defs: dict) -> str:
    """Score a track against mood definitions using weighted features.

    Positive weight: weight * feature_value.
    Negative weight: abs(weight) * (1 - feature_value).

    Args:
        row: Track feature row.
        mood_defs: Mapping of mood name to definition with weights.

    Returns:
        Name of the best-matching mood.
    """
    best_mood = "Unknown"
    best_score = -float("inf")
    for mood_name, mood_def in mood_defs.items():
        score = 0.0
        for feat, weight in mood_def["weights"].items():
            col = FEATURE_COLS.get(feat, feat)
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


class MoodClassifier:
    """Classify tracks into universal moods and genre-specific sub-labels."""

    def __init__(self, storage_config: StorageConfig) -> None:
        """Initialize MoodClassifier with storage configuration.

        Args:
            storage_config: StorageConfig instance for Delta Lake access.
        """
        self._storage = storage_config
        self._opts = storage_config.to_dict()

    def classify_track(self, row: pd.Series) -> dict:
        """Classify a single track into genre family, universal mood, and sub-label.

        Args:
            row: Series with audio features and track_genre.

        Returns:
            Dict with genre_family, mood, and genre_sub_label keys.
        """
        genre = str(row.get("track_genre", "")).lower()
        family = GENRE_FAMILY_MAP.get(genre, "Pop")
        mood = classify_universal_mood(
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
            genre_family=family,
        )
        sub_moods = GENRE_SUB_MOODS.get(family, {})
        sub_label = _score_mood(row, sub_moods) if sub_moods else "Unknown"
        return {"genre_family": family, "mood": mood, "genre_sub_label": sub_label}

    def run(self, feast_data_path: str) -> str:
        """Run full mood classification pipeline.

        Reads silver tracks, classifies each track, writes Feast parquet,
        and updates gold dim_track and fact_streams with mood_cluster.

        Args:
            feast_data_path: Directory path for Feast parquet output.

        Returns:
            Path to the written Feast parquet file.
        """
        tracks = DeltaTable(Paths.SILVER_TRACKS, storage_options=self._opts).to_pandas()

        df = tracks.copy()
        df["loudness_norm"] = ((df["loudness"].fillna(-30) + 60) / 60).clip(0, 1)
        df["mode"] = df["mode"].fillna(0).astype(float)
        df["tempo"] = df["tempo"].fillna(120)
        df["tempo_norm"] = ((df["tempo"] - 50) / 150).clip(0, 1)
        df["genre_family"] = df["track_genre"].str.lower().map(GENRE_FAMILY_MAP).fillna("Pop")

        df["mood_cluster"] = df.apply(lambda r: _score_mood(r, GENRE_SUB_MOODS.get(r["genre_family"], {})), axis=1)
        df["mood"] = df.apply(
            lambda r: classify_universal_mood(
                energy=r.get("energy", 0.5),
                valence=r.get("valence", 0.5),
                danceability=r.get("danceability", 0.5),
                acousticness=r.get("acousticness", 0.0),
                instrumentalness=r.get("instrumentalness", 0.0),
                tempo_norm=r.get("tempo_norm", 0.5),
                loudness_norm=r.get("loudness_norm", 0.5),
                speechiness=r.get("speechiness", 0.0),
                liveness=r.get("liveness", 0.0),
                mode=r.get("mode", 0),
                genre_family=r.get("genre_family", "Pop"),
            ),
            axis=1,
        )

        os.makedirs(feast_data_path, exist_ok=True)
        feast_df = df[["track_id", "mood_cluster", "mood", "genre_family", "danceability", "energy", "valence"]].copy()
        feast_df["event_timestamp"] = pd.Timestamp.now(tz="UTC")
        feast_path = os.path.join(feast_data_path, "track_features.parquet")
        feast_df.to_parquet(feast_path, index=False)

        mood_map = df.set_index("track_id")["mood_cluster"]

        dim_track = DeltaTable(f"{Paths.GOLD_BASE}/dim_track", storage_options=self._opts).to_pandas()
        dim_track["mood_cluster"] = dim_track["track_id"].map(mood_map)
        write_deltalake(f"{Paths.GOLD_BASE}/dim_track", dim_track, mode="overwrite", storage_options=self._opts)

        fact = DeltaTable(f"{Paths.GOLD_BASE}/fact_streams", storage_options=self._opts).to_pandas()
        fact["mood_cluster"] = fact["track_id"].map(mood_map)
        write_deltalake(f"{Paths.GOLD_BASE}/fact_streams", fact, mode="overwrite", storage_options=self._opts)

        return feast_path
