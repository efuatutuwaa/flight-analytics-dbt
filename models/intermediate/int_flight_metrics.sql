-- Model: int_flight_metrics
-- Grain: 1 row per flight_id
-- Purpose:
--   Pre-classify each flight with performance flags and operated-only columns.
--   Defines the on-time / delayed / cancelled business rules once.
--   All marts aggregate from this model — no metric logic is repeated downstream.

select
    -- identifiers and join keys
    flight_id,
    operating_airline_id,
    departure_airport_id,
    arrival_airport_id,
    route_key,
    flight_date,

    -- raw metrics (passed through for aggregation)
    is_cancelled,
    cast(is_cancelled as int)   as cancelled_flag,
    route_distance_km,

    -- delay and duration nulled for cancelled flights so avg() excludes them cleanly
    case when is_cancelled = false then departure_delay_minutes end  as departure_delay_minutes_operated,
    case when is_cancelled = false then arrival_delay_minutes end    as arrival_delay_minutes_operated,
    case when is_cancelled = false then scheduled_duration_minutes end as scheduled_duration_minutes_operated,
    case when is_cancelled = false then actual_duration_minutes end  as actual_duration_minutes_operated,

    -- on-time flags: industry standard threshold is 15 minutes
    case when departure_delay_minutes < 15 and is_cancelled = false then 1 else 0 end as is_on_time_departure,
    case when departure_delay_minutes >= 15 and is_cancelled = false then 1 else 0 end as is_delayed_departure,
    case when arrival_delay_minutes < 15 and is_cancelled = false then 1 else 0 end as is_on_time_arrival,
    case when arrival_delay_minutes >= 15 and is_cancelled = false then 1 else 0 end as is_delayed_arrival

from {{ ref('fct_flights') }}
