-- This model defines the dim_route dimension table, which represents unique flight routes
-- between two airports.

-- Grain: 1 row per route_key (unique departure_airport_id → arrival_airport_id pair)

-- The dimension contains attributes describing the route itself rather than individual flights.
-- It is used by fct_flights to analyze route-level performance such as delays, traffic volume,
-- and route efficiency.

-- Typical analytical use cases:
-- - Identify busiest routes
-- - Compare airline performance across routes
-- - Analyze route efficiency and distance
-- - Evaluate airport network connectivity


with routes as (

    select
        route_key,
        departure_airport_id,
        arrival_airport_id,
        avg(route_distance_km) as route_distance_km
    from {{ ref('int_flight_routes') }}
    group by route_key, departure_airport_id, arrival_airport_id

)

select 
    r.route_key,
    r.departure_airport_id,
    r.arrival_airport_id,
    r.route_distance_km,
    concat(dep.airport_iata_code, ' → ', arr.airport_iata_code) as route_label,
    dep.country_id != arr.country_id as is_international

from routes r 
left join {{ ref('dim_airport') }} dep
    on r.departure_airport_id = dep.airport_id
left join {{ ref('dim_airport') }} arr
    on r.arrival_airport_id = arr.airport_id
