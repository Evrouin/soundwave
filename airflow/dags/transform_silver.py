"""Bronze-to-silver transformation DAG.

Daily transformation: deduplicate, enforce schema, validate audio feature
ranges, build SCD Type 2 artist dimension, and run data quality checks.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    dag_id="transform_silver",
    start_date=datetime(2026, 1, 1),
    schedule="0 7 * * *",
    catchup=False,
    tags=["soundwave", "transform", "silver"],
)
def transform_silver():
    """Transform bronze data into validated silver layer."""

    @task()
    def run_transform():
        """Deduplicate, enforce schema, validate, and write silver tables."""
        from soundwave.config.storage import StorageConfig
        from soundwave.pipeline.silver import SilverTransformer

        transformer = SilverTransformer(StorageConfig())
        transformer.transform()
        transformer.build_artist_dimension()

    @task()
    def run_data_quality():
        """Validate silver layer integrity."""
        from soundwave.config.storage import StorageConfig
        from soundwave.pipeline.silver import SilverTransformer

        SilverTransformer(StorageConfig()).validate()

    run_transform() >> run_data_quality()


transform_silver()
