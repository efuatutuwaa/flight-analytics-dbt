-- Model: int_flight_routes_metrics
-- Grain: 1 row per flight_id
-- Purpose:
--   Derive delay, duration, and status metrics from int_flight_routes_spine.
--   All flight timing business logic lives here.
--   Values outside plausible ranges are nulled to exclude source API artefacts.

select
    -- passthrough identifiers and keys
    flight_id,
    route_key,
    departure_airport_id,
    arrival_airport_id,
    flight_status,
    flight_date,
    flight_number,

    -- timezone context
    departure_airport_timezone,
    arrival_airport_timezone,

    -- route geometry
    route_distance_km,

    -- local times
    scheduled_departure_time_local,
    scheduled_arrival_time_local,
    actual_departure_time_local,
    actual_arrival_time_local,

    -- utc times
    scheduled_departure_time_utc,
    scheduled_arrival_time_utc,
    actual_departure_time_utc,
    actual_arrival_time_utc,

    -- terminal and gate info
    departure_terminal,
    departure_gate,
    arrival_terminal,
    arrival_gate,
    arrival_baggage_claim,

    -- status flags
    (flight_status = 'cancelled') as is_cancelled,

    -- Some API records mark flights as "landed" but do not provide an actual arrival time.
    -- In such cases we classify the status as "data unavailable" to signal incomplete data.
    case
        when flight_status = 'cancelled' then 'cancelled'
        when actual_arrival_time_utc is null then 'data unavailable'
        else 'landed'
    end as flight_status_clean,

    -- duration metrics (null-guarded)
    case
        when scheduled_departure_time_utc is null
            or scheduled_arrival_time_utc is null then null
        else timestampdiff(minute, scheduled_departure_time_utc, scheduled_arrival_time_utc)
    end as scheduled_duration_minutes,

    case
        when actual_departure_time_utc is null
            or actual_arrival_time_utc is null then null
        else timestampdiff(minute, actual_departure_time_utc, actual_arrival_time_utc)
    end as actual_duration_minutes,

    -- delay metrics (null-guarded + range-clamped to exclude timezone/API artefacts)
    case
        when actual_departure_time_utc is null
            or scheduled_departure_time_utc is null then null
        when timestampdiff(minute, scheduled_departure_time_utc, actual_departure_time_utc)
            not between -120 and 1440 then null
        else timestampdiff(minute, scheduled_departure_time_utc, actual_departure_time_utc)
    end as departure_delay_minutes,

    case
        when actual_arrival_time_utc is null
            or scheduled_arrival_time_utc is null then null
        when timestampdiff(minute, scheduled_arrival_time_utc, actual_arrival_time_utc)
            not between -120 and 1440 then null
        else timestampdiff(minute, scheduled_arrival_time_utc, actual_arrival_time_utc)
    end as arrival_delay_minutes

from {{ ref('int_flight_routes_spine') }}
