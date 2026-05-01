from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, String

track = Entity(name="track", join_keys=["track_id"])

track_source = FileSource(
    path="/opt/airflow/feast/data/track_features.parquet",
    timestamp_field="event_timestamp",
)

track_features = FeatureView(
    name="track_features",
    entities=[track],
    ttl=timedelta(days=7),
    schema=[
        Field(name="mood_cluster", dtype=String),
        Field(name="genre_family", dtype=String),
        Field(name="danceability", dtype=Float32),
        Field(name="energy", dtype=Float32),
        Field(name="valence", dtype=Float32),
    ],
    source=track_source,
)
