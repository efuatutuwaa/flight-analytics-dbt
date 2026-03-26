-- Model: int_flight_routes
-- Grain: 1 row per flight_id
-- Purpose:
--   Combine route geometry, airport timezone data,
--   and departure/arrival timestamps to calculate
--   flight duration and delay metrics.

with base as (
    select
        -- identifiers --
        fd.flight_id,
        fd.departure_airport_id,
        fd.arrival_airport_id,
        fd.flight_status,
        fd.flight_date,
        fd.flight_number,
        dep_geo.airport_timezone_name as departure_airport_timezone,
        arr_geo.airport_timezone_name as arrival_airport_timezone,

        -- route key --
        concat(fd.departure_airport_id, '-', fd.arrival_airport_id) as route_key,

        -- routes geometry --
        r.route_distance_km,

        -- scheduled times in local time --
        dep.scheduled_departure_time as scheduled_departure_time_local,
        arr.scheduled_arrival_time as scheduled_arrival_time_local,

        -- actual times in local time --
        dep.actual_departure_time as actual_departure_time_local,
        arr.actual_arrival_time as actual_arrival_time_local,

        -- terminal and gate info --
        dep.departure_terminal,
        dep.departure_gate,
        arr.arrival_terminal,
        arr.arrival_gate,
        arr.arrival_baggage_claim,

        -- scheduled times in utc --
        to_utc_timestamp(
            dep.scheduled_departure_time,
            coalesce(nullif(nullif(dep_geo.airport_timezone_name, ''), 'Unknown'), 'UTC')
        ) as scheduled_departure_time_utc,

        to_utc_timestamp(
            arr.scheduled_arrival_time,
            coalesce(nullif(nullif(arr_geo.airport_timezone_name, ''), 'Unknown'), 'UTC')
        ) as scheduled_arrival_time_utc,

        -- actual times in utc --
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

)

select
    -- identifiers --
    flight_id,
    route_key,
    departure_airport_id,
    arrival_airport_id,
    flight_status,
    flight_date,
    flight_number,

    -- timezone context --
    departure_airport_timezone,
    arrival_airport_timezone,

    -- route geometry --
    route_distance_km,

    -- local times --
    scheduled_departure_time_local,
    scheduled_arrival_time_local,
    actual_departure_time_local,
    actual_arrival_time_local,

    -- utc times --
    scheduled_departure_time_utc,
    scheduled_arrival_time_utc,
    actual_departure_time_utc,
    actual_arrival_time_utc,

    -- derived metrics --
    case
        when scheduled_departure_time_utc is null
            or scheduled_arrival_time_utc is null then null
        else timestampdiff(minute,
            scheduled_departure_time_utc,
            scheduled_arrival_time_utc)
    end as scheduled_duration_minutes,

    case
        when actual_departure_time_utc is null
            or actual_arrival_time_utc is null then null
        else timestampdiff(minute,
            actual_departure_time_utc,
            actual_arrival_time_utc)
    end as actual_duration_minutes,
   

    case
        when actual_departure_time_utc is null
            or scheduled_departure_time_utc is null then null
        else timestampdiff(minute,
            scheduled_departure_time_utc,
            actual_departure_time_utc)
    end as departure_delay_minutes,

    case
        when actual_arrival_time_utc is null
            or scheduled_arrival_time_utc is null then null
        else timestampdiff(minute,
            scheduled_arrival_time_utc,
            actual_arrival_time_utc)
    end as arrival_delay_minutes,

    -- terminal and gate info --
    departure_terminal,
    departure_gate,
    arrival_terminal,
    arrival_gate,
    arrival_baggage_claim,

    -- status flags --
    (flight_status = 'cancelled') as is_cancelled,

    -- Some API records mark flights as "landed" but do not provide an actual arrival time.
    -- In such cases we classify the status as "data unavailable" to signal incomplete data.
    case
        when flight_status = 'cancelled' then 'cancelled'
        when actual_arrival_time_utc is null then 'data unavailable'
        else 'landed'
    end as flight_status_clean
from base;


