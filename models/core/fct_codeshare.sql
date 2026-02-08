
with codeshare_details as (

    select 
        flight_id,
        operating_airline_id,
        marketing_airline_id

    from {{ref ('int_flight_codeshare')}}
)

select 
    *

from codeshare_details