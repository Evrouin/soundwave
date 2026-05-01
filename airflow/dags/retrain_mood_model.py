"""Weekly mood classification and Feast materialization DAG.

Two-tier classification: universal mood (10 labels) + genre sub-label (50 labels).
Writes to Feast offline store, materializes to Redis, updates gold tables.
"""

import os

from airflow.decorators import dag, task
from pendulum import datetime

FEAST_REPO = "/opt/airflow/feast"


@dag(
    dag_id="retrain_mood_model",
    start_date=datetime(2026, 1, 1),
    schedule="0 0 * * 0",
    catchup=False,
    tags=["soundwave", "ml", "retrain"],
)
def retrain_mood_model():
    """Classify tracks and materialize features to Redis."""

    @task()
    def train_mood_classifier() -> str:
        """Run two-tier mood classification on all silver tracks."""
        from soundwave.classifier.mood import MoodClassifier
        from soundwave.config.paths import Paths
        from soundwave.config.storage import StorageConfig

        classifier = MoodClassifier(StorageConfig())
        return classifier.run(Paths.FEAST_DATA)

    @task()
    def materialize_feast_features(feast_path: str):
        """Apply Feast definitions and materialize to Redis online store."""
        from datetime import datetime as dt
        from datetime import timedelta, timezone
        from importlib.util import module_from_spec, spec_from_file_location

        from feast import FeatureStore

        store = FeatureStore(repo_path=FEAST_REPO)

        spec = spec_from_file_location("features", os.path.join(FEAST_REPO, "features.py"))
        mod = module_from_spec(spec)
        spec.loader.exec_module(mod)

        store.apply([mod.track, mod.track_features])
        now = dt.now(timezone.utc)
        store.materialize(start_date=now - timedelta(days=365), end_date=now)
        print("Features materialized to Redis")

    feast_path = train_mood_classifier()
    materialize_feast_features(feast_path)


retrain_mood_model()
