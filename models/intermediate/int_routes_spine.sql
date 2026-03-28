-- Model: int_routes_spine
-- Grain: 1 row per route_key (unique departure_airport_id → arrival_airport_id pair)
-- Purpose:
--   Collapse flight-level route data to route grain.
--   Distance is averaged across flights as the source provides it per flight record.

select
    route_key,
    departure_airport_id,
    arrival_airport_id,
    avg(route_distance_km) as route_distance_km
from {{ ref('int_flight_routes_spine') }}
group by route_key, departure_airport_id, arrival_airport_id
