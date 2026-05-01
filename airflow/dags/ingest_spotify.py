"""
DAG: ingest_spotify
Daily ingestion from the Spotify API into the bronze Delta Lake layer.
Runs at 06:00 UTC every day.
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
    @task()
    def authenticate() -> str:
        """Authenticate with the Spotify API and return an access token."""
        raise NotImplementedError("Implement Spotify OAuth")

    @task()
    def fetch_new_releases(token: str) -> str:
        """Fetch new album/track releases from Spotify."""
        raise NotImplementedError("Implement new releases fetch")

    @task()
    def fetch_audio_features(token: str, releases: str) -> str:
        """Fetch audio features for the newly released tracks."""
        raise NotImplementedError("Implement audio features fetch")

    @task()
    def load_to_bronze(releases: str, features: str):
        """Write raw Spotify data into bronze Delta Lake."""
        raise NotImplementedError("Implement bronze Delta Lake write")

    token = authenticate()
    releases = fetch_new_releases(token)
    features = fetch_audio_features(token, releases)
    load_to_bronze(releases, features)


ingest_spotify()
