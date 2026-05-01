select
    genre_id,
    genre_name
from {{ source('silver', 'genres') }}
