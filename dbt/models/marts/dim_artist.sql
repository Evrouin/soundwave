select
    {{ dbt_utils.generate_surrogate_key(['artist_id', 'valid_from']) }} as artist_sk,
    artist_id,
    artist_name,
    artist_genre,
    artist_popularity,
    valid_from,
    valid_to,
    is_current
from {{ source('silver', 'artists_scd2') }}
