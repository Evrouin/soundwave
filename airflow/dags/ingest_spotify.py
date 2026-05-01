"""Spotify API daily ingestion DAG.

Fetches new releases from the Spotify Web API, retrieves audio features,
and writes to the bronze Delta Lake layer. Runs daily at 06:00 UTC.
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
        """Authenticate, fetch new releases with audio features, and load to bronze."""
        import pandas as pd
        from deltalake import write_deltalake

        from soundwave.config.paths import Paths
        from soundwave.config.storage import StorageConfig
        from soundwave.pipeline.spotify import SpotifyClient

        client = SpotifyClient()
        rows = client.ingest_new_releases()

        if not rows:
            print("No new releases found")
            return

        df = pd.DataFrame(rows)
        storage = StorageConfig()
        write_deltalake(Paths.BRONZE_TRACKS, df, mode="append", storage_options=storage.to_dict())
        print(f"Appended {len(df)} rows to bronze")

    fetch_and_load()


ingest_spotify()
