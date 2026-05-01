select
    stream_id,
    track_id,
    artist_id,
    genre_id,
    ingestion_date,
    popularity,
    mood_cluster
from {{ source('silver', 'streams') }}
