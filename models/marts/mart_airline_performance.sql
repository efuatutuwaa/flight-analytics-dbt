-- Mart: mart_airline_performance
-- Grain: 1 row per operating_airline_id

select
    -- grain key
    f.operating_airline_id,

    -- dimension attributes
    a.airline_name,
    a.airline_icao_code,
    a.airline_type,
    a.airline_fleet_size,
    a.airline_fleet_average_age,

    -- volume metrics
    count(f.flight_id)                                      as total_flights,
    sum(f.cancelled_flag)                                   as cancelled_flights,
    count(distinct f.route_key)                             as total_routes_served,

    -- performance metrics
    round(avg(f.departure_delay_minutes_operated), 2)       as avg_departure_delay_minutes,
    round(avg(f.arrival_delay_minutes_operated), 2)         as avg_arrival_delay_minutes,
    round(sum(f.cancelled_flag) / count(f.flight_id), 2)    as cancellation_rate,
    sum(f.is_on_time_departure)                             as on_time_departures,
    sum(f.is_delayed_departure)                             as delayed_departures,
    sum(f.is_on_time_arrival)                               as on_time_arrivals,
    sum(f.is_delayed_arrival)                               as delayed_arrivals

from {{ ref('int_flight_metrics') }} f
left join {{ ref('dim_airline') }} a
    on f.operating_airline_id = a.airline_id
group by
    f.operating_airline_id,
    a.airline_name,
    a.airline_icao_code,
    a.airline_type,
    a.airline_fleet_size,
    a.airline_fleet_average_age
