"""Canonical Delta Lake and Feast path constants."""


class Paths:
    """Central registry of all storage paths used by the Soundwave pipeline."""

    BRONZE_TRACKS: str = "s3://soundwave/bronze/tracks"
    SILVER_TRACKS: str = "s3://soundwave/silver/tracks"
    SILVER_ARTISTS: str = "s3://soundwave/silver/artists"
    GOLD_BASE: str = "s3://soundwave/gold"
    FEAST_REPO: str = "/opt/airflow/feast"
    FEAST_DATA: str = "/opt/airflow/feast/data"
