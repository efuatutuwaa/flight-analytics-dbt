-- Enrich flights with departure and arrival airport attributes --
select 
    fd.flight_id, 
    fd.flight_date,
    fd.flight_number, 
    fd.flight_status,

    fd.departure_airport_id,
    dep.airport_name as departure_airport_name,
    
    fd.arrival_airport_id, 
    arr.airport_name as arrival_airport_name

from {{ ref('stg_flight_details') }} fd

left join {{ ref('stg_airports') }} dep
    on fd.departure_airport_id = dep.airport_id
left join {{ ref('stg_airports') }} arr
    on fd.arrival_airport_id = arr.airport_id;
