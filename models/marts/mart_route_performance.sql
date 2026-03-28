-- Mart: mart_route_performance
-- Grain: 1 row per route_key

select
    -- grain key
    d.route_key,

    -- dimension attributes
    d.route_label,
    d.is_international,
    d.route_distance_km,

    -- volume metrics
    count(f.flight_id)                                      as total_flights,
    sum(f.cancelled_flag)                                   as cancelled_flights,
    count(distinct f.operating_airline_id)                  as airlines_competing,

    -- performance metrics
    round(avg(f.departure_delay_minutes_operated), 2)       as avg_departure_delay_minutes,
    round(avg(f.arrival_delay_minutes_operated), 2)         as avg_arrival_delay_minutes,
    round(avg(f.scheduled_duration_minutes_operated), 2)    as avg_scheduled_duration_minutes,
    round(avg(f.actual_duration_minutes_operated), 2)       as avg_actual_duration_minutes,
    round(sum(f.cancelled_flag) / count(f.flight_id), 2)    as cancellation_rate,
    sum(f.is_on_time_departure)                             as on_time_departures,
    sum(f.is_delayed_departure)                             as delayed_departures,
    sum(f.is_on_time_arrival)                               as on_time_arrivals,
    sum(f.is_delayed_arrival)                               as delayed_arrivals

from {{ ref('dim_routes') }} d
left join {{ ref('int_flight_metrics') }} f
    on f.route_key = d.route_key
group by
    d.route_key,
    d.route_label,
    d.is_international,
    d.route_distance_km
