"""Bronze layer ingestion for raw data sources."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from deltalake import write_deltalake

from soundwave.config.paths import Paths
from soundwave.config.storage import StorageConfig


class BronzeLoader:
    """Loads raw data into the bronze Delta Lake layer."""

    def __init__(self, storage_config: StorageConfig) -> None:
        """Initialise with storage configuration.

        Args:
            storage_config: MinIO/S3 credentials and endpoint.
        """
        self.storage = storage_config

    def ingest_kaggle(self, csv_path: str) -> int:
        """Read a Kaggle CSV and write it to the bronze tracks Delta table.

        Args:
            csv_path: Local filesystem path to the CSV file.

        Returns:
            Number of rows written.
        """
        df = pd.read_csv(csv_path)
        if "Unnamed: 0" in df.columns:
            df = df.drop(columns=["Unnamed: 0"])
        df["ingestion_timestamp"] = datetime.now(timezone.utc)
        df["source"] = "kaggle"
        write_deltalake(Paths.BRONZE_TRACKS, df, mode="overwrite", storage_options=self.storage.to_dict())
        return len(df)
