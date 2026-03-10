-- Mart: mart_flight_performance
-- Grain: 1 row per flight_date + route_key + operating_airline_id

select
    -- grain keys --
    f.route_key,
    f.operating_airline_id,
    f.flight_date,

    -- dimension attributes --
    a.airline_name,
    a.airline_icao_code,
    r.route_label,
    r.is_international,

    -- volume metric --
    count(f.flight_id) as total_flights,
    sum(cast(f.is_cancelled as int)) as cancelled_flights,

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
    left join {{ ref('dim_routes') }} r
        on f.route_key = r.route_key
group by f.route_key,
            f.operating_airline_id,
            a.airline_name, 
            a.airline_icao_code,
            r.route_label, 
            r.is_international,
            f.flight_date
