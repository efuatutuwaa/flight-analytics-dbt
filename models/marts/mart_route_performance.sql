-- Mart: mart_route_performance
-- Grain: 1 row per route_key

select 
    -- grain key -- 
    d.route_key, 

    -- dimension attributes -- 
    d.route_label, 
    d.is_international, 
    d.route_distance_km, 

    -- volume metrics --
    count(f.flight_id) as total_flights,
    sum(case when f.is_cancelled = true then 1 else 0 end) as cancelled_flights, 
    count(distinct f.operating_airline_id) as airlines_competing, 

    -- performance metrics --
    round(avg(case when f.is_cancelled = false then f.departure_delay_minutes end), 2) as avg_departure_delay_minutes,
    round(avg(case when f.is_cancelled = false then f.arrival_delay_minutes end), 2) as avg_arrival_delay_minutes,
    round(sum(cast(f.is_cancelled as int)) / count(f.flight_id), 2) as cancellation_rate,
    sum(case when f.departure_delay_minutes < 15 and f.is_cancelled = false then 1 else 0 end) as on_time_departures,
    sum(case when f.departure_delay_minutes >= 15 and f.is_cancelled = false then 1 else 0 end) as delayed_departures,
    sum(case when f.arrival_delay_minutes < 15 and f.is_cancelled = false then 1 else 0 end) as on_time_arrivals,
    sum(case when f.arrival_delay_minutes >= 15 and f.is_cancelled = false then 1 else 0 end) as delayed_arrivals   
from {{ ref('dim_routes') }} d
left join {{ ref('fct_flights') }} f
    on f.route_key = d.route_key
group by d.route_key,
         d.route_label,
         d.is_international,
         d.route_distance_km


