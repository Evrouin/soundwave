"""
DAG: ingest_kaggle
One-time bulk ingestion of Kaggle Spotify Tracks dataset into the bronze Delta Lake layer.
Manually triggered (no schedule). Downloads CSV via Kaggle API, then writes to
bronze Delta table on MinIO with ingestion metadata columns.
"""
import os

from airflow.decorators import dag, task
from pendulum import datetime


KAGGLE_DATASET = "maharshipandya/-spotify-tracks-dataset"
LOCAL_DIR = "/tmp/kaggle_ingest"
BRONZE_PATH = "s3a://soundwave/bronze/tracks"


@dag(
    dag_id="ingest_kaggle",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["soundwave", "ingestion", "kaggle"],
    doc_md=__doc__,
)
def ingest_kaggle():

    @task()
    def download_dataset() -> str:
        """Download CSV dataset from Kaggle API and return local file path."""
        from kaggle.api.kaggle_api_extended import KaggleApi

        os.makedirs(LOCAL_DIR, exist_ok=True)
        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(KAGGLE_DATASET, path=LOCAL_DIR, unzip=True)

        csv_path = os.path.join(LOCAL_DIR, "dataset.csv")
        if not os.path.exists(csv_path):
            for f in os.listdir(LOCAL_DIR):
                if f.endswith(".csv"):
                    csv_path = os.path.join(LOCAL_DIR, f)
                    break
        row_count = sum(1 for _ in open(csv_path)) - 1
        print(f"Downloaded {row_count} rows to {csv_path}")
        return csv_path

    @task()
    def load_to_bronze(csv_path: str):
        """Read CSV with pandas and write to bronze Delta table on MinIO."""
        import pandas as pd
        from datetime import datetime as dt, timezone
        from deltalake import write_deltalake

        storage_options = {
            "AWS_ENDPOINT_URL": "http://minio:9000",
            "AWS_ACCESS_KEY_ID": os.environ.get("MINIO_ROOT_USER", "minio"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "minio123"),
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "AWS_ALLOW_HTTP": "true",
        }

        df = pd.read_csv(csv_path)

        # Drop unnamed index column if present
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])

        # Add ingestion metadata (FR-ING-04)
        df["ingestion_timestamp"] = dt.now(timezone.utc)
        df["source"] = "kaggle"

        write_deltalake(
            BRONZE_PATH.replace("s3a://", "s3://"),
            df,
            mode="overwrite",
            storage_options=storage_options,
        )
        print(f"Loaded {len(df)} rows to bronze Delta table")

    csv_path = download_dataset()
    load_to_bronze(csv_path)


ingest_kaggle()
