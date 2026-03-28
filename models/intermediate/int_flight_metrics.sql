-- Model: int_flight_metrics
-- Grain: 1 row per flight_id
-- Purpose:
--   Pre-classify each flight with performance flags and operated-only columns.
--   Defines the on-time / delayed / cancelled business rules once.
--   All marts aggregate from this model — no metric logic is repeated downstream.

select
    -- identifiers and join keys
    r.flight_id,
    a.airline_id                as operating_airline_id,
    r.departure_airport_id,
    r.arrival_airport_id,
    r.route_key,
    r.flight_date,

    -- raw metrics (passed through for aggregation)
    r.is_cancelled,
    cast(r.is_cancelled as int) as cancelled_flag,
    r.route_distance_km,

    -- delay and duration nulled for cancelled flights so avg() excludes them cleanly
    case when r.is_cancelled = false then r.departure_delay_minutes end  as departure_delay_minutes_operated,
    case when r.is_cancelled = false then r.arrival_delay_minutes end    as arrival_delay_minutes_operated,
    case when r.is_cancelled = false then r.scheduled_duration_minutes end as scheduled_duration_minutes_operated,
    case when r.is_cancelled = false then r.actual_duration_minutes end  as actual_duration_minutes_operated,

    -- on-time flags: industry standard threshold is 15 minutes
    case when r.departure_delay_minutes < 15 and r.is_cancelled = false then 1 else 0 end as is_on_time_departure,
    case when r.departure_delay_minutes >= 15 and r.is_cancelled = false then 1 else 0 end as is_delayed_departure,
    case when r.arrival_delay_minutes < 15 and r.is_cancelled = false then 1 else 0 end as is_on_time_arrival,
    case when r.arrival_delay_minutes >= 15 and r.is_cancelled = false then 1 else 0 end as is_delayed_arrival

from {{ ref('int_flight_routes_metrics') }} r
left join {{ ref('int_flight_airline') }} a
    on r.flight_id = a.flight_id
