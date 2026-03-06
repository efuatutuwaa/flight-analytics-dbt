
-- flight and airline details intermediate model --
select 
    fd.flight_id, 
    fd.flight_date,
    fd.flight_number,
    fd.flight_status, 
    fd.airline_id,
    a.airline_iata_code,
    a.airline_icao_code,
    a.airline_name,
    a.airline_type,
    a.airline_country_name,
    a.airline_fleet_size 
from {{ ref('stg_flight_details') }} fd
left join {{ ref('stg_airline') }} a 
    on fd.airline_id = a.airline_id

