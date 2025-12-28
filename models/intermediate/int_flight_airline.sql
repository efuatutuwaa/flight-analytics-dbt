
-- flight and airline details intermediate model --
select 
    fd.flight_id, 
    fd.airline_id,
    fd.flight_date,
    fd.flight_number,
    fd.flight_status, 
    a.airline_name,
    a.airline_type  
from {{ ref('stg_flight_details') }} fd
left join {{ ref('stg_airline') }} a 
    on fd.airline_id = a.airline_id;

