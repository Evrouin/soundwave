select
    genre_id,
    ingestion_date,
    avg(popularity) as avg_popularity,
    avg(danceability) as avg_danceability,
    avg(energy) as avg_energy
from {{ ref('fact_streams') }}
join {{ ref('dim_track') }} using (track_id)
group by genre_id, ingestion_date
