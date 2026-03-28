-- Model: int_flight_routes_spine
-- Grain: 1 row per flight_id
-- Purpose:
--   Join flight details with route geometry, departure/arrival timestamps,
--   and airport timezone data. UTC conversion applied here.
--   No derived metrics — see int_flight_routes_metrics for delay and duration.

select
    -- identifiers
    fd.flight_id,
    fd.departure_airport_id,
    fd.arrival_airport_id,
    fd.flight_status,
    fd.flight_date,
    fd.flight_number,

    -- route key
    concat(fd.departure_airport_id, '-', fd.arrival_airport_id) as route_key,

    -- timezone context
    dep_geo.airport_timezone_name as departure_airport_timezone,
    arr_geo.airport_timezone_name as arrival_airport_timezone,

    -- route geometry
    r.route_distance_km,

    -- local times
    dep.scheduled_departure_time as scheduled_departure_time_local,
    arr.scheduled_arrival_time   as scheduled_arrival_time_local,
    dep.actual_departure_time    as actual_departure_time_local,
    arr.actual_arrival_time      as actual_arrival_time_local,

    -- terminal and gate info
    dep.departure_terminal,
    dep.departure_gate,
    arr.arrival_terminal,
    arr.arrival_gate,
    arr.arrival_baggage_claim,

    -- utc times
    to_utc_timestamp(
        dep.scheduled_departure_time,
        coalesce(nullif(nullif(dep_geo.airport_timezone_name, ''), 'Unknown'), 'UTC')
    ) as scheduled_departure_time_utc,

    to_utc_timestamp(
        arr.scheduled_arrival_time,
        coalesce(nullif(nullif(arr_geo.airport_timezone_name, ''), 'Unknown'), 'UTC')
    ) as scheduled_arrival_time_utc,

    to_utc_timestamp(
        dep.actual_departure_time,
        coalesce(nullif(nullif(dep_geo.airport_timezone_name, ''), 'Unknown'), 'UTC')
    ) as actual_departure_time_utc,

    to_utc_timestamp(
        arr.actual_arrival_time,
        coalesce(nullif(nullif(arr_geo.airport_timezone_name, ''), 'Unknown'), 'UTC')
    ) as actual_arrival_time_utc

from {{ ref('stg_flight_details') }} fd
left join {{ ref('stg_routes') }} r
    on fd.flight_id = r.flight_id
left join {{ ref('stg_departures') }} dep
    on fd.flight_id = dep.flight_id
left join {{ ref('stg_arrivals') }} arr
    on fd.flight_id = arr.flight_id
left join {{ ref('int_airport_geography') }} dep_geo
    on fd.departure_airport_id = dep_geo.airport_id
left join {{ ref('int_airport_geography') }} arr_geo
    on fd.arrival_airport_id = arr_geo.airport_id
