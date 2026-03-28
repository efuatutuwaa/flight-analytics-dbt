-- Model: dim_routes
-- Grain: 1 row per route_key (unique departure_airport_id → arrival_airport_id pair)
-- Purpose:
--   Route dimension providing geometry, labels, and international flag.
--   Used by fct_flights and marts to analyse route-level performance,
--   network connectivity, and route efficiency.

select
    r.route_key,
    r.departure_airport_id,
    r.arrival_airport_id,
    r.route_distance_km,
    concat(dep.airport_iata_code, ' → ', arr.airport_iata_code) as route_label,
    dep.country_id != arr.country_id                            as is_international

from {{ ref('int_routes_spine') }} r
left join {{ ref('dim_airport') }} dep
    on r.departure_airport_id = dep.airport_id
left join {{ ref('dim_airport') }} arr
    on r.arrival_airport_id = arr.airport_id
