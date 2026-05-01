select
    track_id,
    track_name,
    duration_ms,
    danceability,
    energy,
    valence,
    tempo,
    acousticness,
    instrumentalness,
    mood_cluster
from {{ source('silver', 'tracks') }}
