"""Kaggle bulk ingestion DAG.

One-time manual trigger to download the Spotify Tracks dataset
from Kaggle and load it into the bronze Delta Lake layer.
"""

import os

from airflow.decorators import dag, task
from pendulum import datetime

KAGGLE_DATASET = "maharshipandya/-spotify-tracks-dataset"
LOCAL_DIR = "/tmp/kaggle_ingest"


@dag(
    dag_id="ingest_kaggle",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["soundwave", "ingestion", "kaggle"],
)
def ingest_kaggle():
    """Download Kaggle CSV and write to bronze Delta table on MinIO."""

    @task()
    def download_dataset() -> str:
        """Download and unzip the Kaggle Spotify Tracks dataset."""
        from kaggle.api.kaggle_api_extended import KaggleApi

        os.makedirs(LOCAL_DIR, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(KAGGLE_DATASET, path=LOCAL_DIR, unzip=True)

        for f in os.listdir(LOCAL_DIR):
            if f.endswith(".csv"):
                return os.path.join(LOCAL_DIR, f)
        raise FileNotFoundError("No CSV found after Kaggle download")

    @task()
    def load_to_bronze(csv_path: str):
        """Load CSV into bronze Delta table with ingestion metadata."""
        from soundwave.config.storage import StorageConfig
        from soundwave.pipeline.bronze import BronzeLoader

        loader = BronzeLoader(StorageConfig())
        count = loader.ingest_kaggle(csv_path)
        print(f"Loaded {count} rows to bronze")

    csv_path = download_dataset()
    load_to_bronze(csv_path)


ingest_kaggle()
