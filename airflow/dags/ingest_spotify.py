"""Spotify API daily ingestion DAG.

Incremental ingestion of new releases and audio features from the
Spotify Web API. Runs daily at 06:00 UTC.
"""
from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="ingest_spotify",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["soundwave", "ingestion", "spotify"],
)
def ingest_spotify():
    """Fetch new releases from Spotify API and write to bronze Delta table."""

    @task()
    def fetch_and_load():
        """Authenticate, fetch new releases, and load to bronze."""
        raise NotImplementedError("Spotify API ingestion not yet implemented")

    fetch_and_load()


ingest_spotify()
