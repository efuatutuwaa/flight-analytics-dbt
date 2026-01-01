with source as (
    select
        *
    from {{ source ('flights', 'codeshare') }}
),

cleaned as (
    select
        codeshare_id,
        flight_id,
        airline_id as marketing_airline_id,
        airline_name as marketing_airline_name

    from source

)

select
    *
from cleaned