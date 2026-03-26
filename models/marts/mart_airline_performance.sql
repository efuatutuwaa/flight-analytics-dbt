-- Mart: mart_airline_performance
-- Grain: 1 row per operating_airline_id

select 
    -- grain key -- 
    f.operating_airline_id,

    -- dimension attributes --
    a.airline_name,
    a.airline_icao_code,
    a.airline_type,
    a.airline_fleet_size,
    a.airline_fleet_average_age,

    -- volume metrics --
    count(f.flight_id) as total_flights,
    sum(cast(f.is_cancelled as int)) as cancelled_flights,
    count(distinct f.route_key) as total_routes_served,

    -- performance metrics --
    round(avg(case when f.is_cancelled = false then f.departure_delay_minutes end), 2) as avg_departure_delay_minutes,
    round(avg(case when f.is_cancelled = false then f.arrival_delay_minutes end), 2) as avg_arrival_delay_minutes,
    round(sum(cast(f.is_cancelled as int)) / count(f.flight_id), 2) as cancellation_rate,
    sum(case when f.departure_delay_minutes < 15 and f.is_cancelled = false then 1 else 0 end) as on_time_departures,
    sum(case when f.departure_delay_minutes >= 15 and f.is_cancelled = false then 1 else 0 end) as delayed_departures,
    sum(case when f.arrival_delay_minutes < 15 and f.is_cancelled = false then 1 else 0 end) as on_time_arrivals,
    sum(case when f.arrival_delay_minutes >= 15 and f.is_cancelled = false then 1 else 0 end) as delayed_arrivals
from {{ ref('fct_flights') }} f 
left join {{ ref('dim_airline') }} a
    on f.operating_airline_id = a.airline_id
group by f.operating_airline_id,
         a.airline_name,
         a.airline_icao_code,
         a.airline_type,
         a.airline_fleet_size,
         a.airline_fleet_average_age



