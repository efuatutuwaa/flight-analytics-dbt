-- Model: fct_flights
-- Grain: 1 row per flight_id
-- Purpose: Core flight fact table combining airline, route, and timing data
--          to support flight performance, delay, and network analysis.

with flight_keys as (

    select
        flight_id,
        airline_id
    from {{ ref('int_flight_airline') }}

),

route_keys as (

    select
        -- join key --
        flight_id,

        -- identifiers --
        departure_airport_id,
        arrival_airport_id,
        route_key,
        flight_date,
        flight_number,

        -- timestamps --
        scheduled_departure_time_utc,
        scheduled_arrival_time_utc,
        actual_departure_time_utc,
        actual_arrival_time_utc,

        -- status --
        flight_status,
        flight_status_clean,
        is_cancelled,

        -- delay metrics --
        departure_delay_minutes,
        arrival_delay_minutes,

        -- duration metrics --
        scheduled_duration_minutes,
        actual_duration_minutes,

        -- route attributes --
        route_distance_km

    from {{ ref('int_flight_routes') }}

)

select
    -- identifiers --
    fk.flight_id,
    fk.airline_id as operating_airline_id,
    rk.departure_airport_id,
    rk.arrival_airport_id,
    rk.route_key,
    rk.flight_date,
    rk.flight_number,

    -- timestamps --
    rk.scheduled_departure_time_utc,
    rk.scheduled_arrival_time_utc,
    rk.actual_departure_time_utc,
    rk.actual_arrival_time_utc,

    -- status --
    rk.flight_status,
    rk.flight_status_clean,
    rk.is_cancelled,

    -- delay metrics --
    rk.departure_delay_minutes,
    rk.arrival_delay_minutes,

    -- duration and distance --
    rk.scheduled_duration_minutes,
    rk.actual_duration_minutes,
    rk.route_distance_km

from flight_keys fk
left join route_keys rk
    on fk.flight_id = rk.flight_id




