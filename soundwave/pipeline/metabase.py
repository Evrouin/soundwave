"""Export gold layer tables to Postgres for Metabase dashboards."""

import os

from deltalake import DeltaTable
from sqlalchemy import create_engine

from soundwave.config.logger import get_logger
from soundwave.config.paths import Paths
from soundwave.config.storage import StorageConfig

logger = get_logger(__name__)


class MetabaseExporter:
    """Exports gold Delta tables to Postgres for BI consumption."""

    def __init__(self, storage_config: StorageConfig = None, pg_url: str = None):
        """Initialize with storage config and Postgres connection URL."""
        self.storage = storage_config or StorageConfig()
        self.opts = self.storage.to_dict()
        self.pg_url = pg_url or os.environ.get("METABASE_PG_URL", "postgresql://airflow:airflow@postgres:5432/airflow")

    def export_all(self) -> dict[str, int]:
        """Export all gold tables to Postgres. Returns table name → row count."""
        engine = create_engine(self.pg_url)
        tables = {
            "gold_dim_track": f"{Paths.GOLD_BASE}/dim_track",
            "gold_dim_artist": f"{Paths.GOLD_BASE}/dim_artist",
            "gold_dim_genre": f"{Paths.GOLD_BASE}/dim_genre",
            "gold_fact_streams": f"{Paths.GOLD_BASE}/fact_streams",
            "gold_agg_genre_trends": f"{Paths.GOLD_BASE}/agg_genre_trends",
            "gold_agg_artist_profiles": f"{Paths.GOLD_BASE}/agg_artist_profiles",
        }
        counts = {}
        for pg_table, delta_path in tables.items():
            df = DeltaTable(delta_path, storage_options=self.opts).to_pandas()
            df.to_sql(pg_table, engine, if_exists="replace", index=False)
            counts[pg_table] = len(df)
            logger.info("Exported %s: %d rows", pg_table, len(df))
        engine.dispose()
        return counts
