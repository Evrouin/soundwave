"""Gold analytics layer DAG.

Daily build of 6 gold tables from silver: dim_track, dim_artist,
dim_genre, fact_streams, agg_genre_trends, agg_artist_profiles.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="dbt_gold",
    start_date=datetime(2026, 1, 1),
    schedule="0 8 * * *",
    catchup=False,
    tags=["soundwave", "gold", "analytics"],
)
def dbt_gold():
    """Build and validate gold analytics layer."""

    @task()
    def build_gold_models():
        """Build all 6 gold dimension and fact tables."""
        from soundwave.config.storage import StorageConfig
        from soundwave.pipeline.gold import GoldBuilder

        builder = GoldBuilder(StorageConfig())
        counts = builder.build()
        for table, count in counts.items():
            print(f"{table}: {count} rows")

    @task()
    def run_gold_tests():
        """Validate PK uniqueness and not-null constraints."""
        from soundwave.config.storage import StorageConfig
        from soundwave.pipeline.gold import GoldBuilder

        GoldBuilder(StorageConfig()).validate()

    build_gold_models() >> run_gold_tests()


dbt_gold()
