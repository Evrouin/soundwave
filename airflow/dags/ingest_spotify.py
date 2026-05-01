"""Spotify API daily ingestion DAG.

Fetches new releases from the Spotify Web API and writes to bronze Delta table.
Requires Spotify Extended Quota Mode for full endpoint access.
Currently disabled — the Kaggle dataset serves as the primary data source.
"""
from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="ingest_spotify",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["soundwave", "ingestion", "spotify"],
)
def ingest_spotify():
    """Fetch new releases from Spotify API and write to bronze Delta table."""

    @task()
    def fetch_and_load():
        """Authenticate, fetch new releases, and load to bronze.

        Requires Spotify Extended Quota Mode for access to:
        - /browse/new-releases
        - /audio-features
        - /tracks (bulk)
        """
        raise NotImplementedError(
            "Spotify API ingestion requires Extended Quota Mode. "
            "Apply at https://developer.spotify.com/dashboard"
        )

    fetch_and_load()


ingest_spotify()
