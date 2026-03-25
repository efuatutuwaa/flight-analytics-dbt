-- Mart: mart_airport_operations
-- Grain: 1 row per airport_id

with departures as (
    select
        departure_airport_id as airport_id,
        count(flight_id) as total_departures,
        sum(case when is_cancelled = true then 1 else 0 end) as cancelled_departures,
        round(avg(case when is_cancelled = false then departure_delay_minutes end), 2) as avg_departure_delay_minutes,
        sum(case when departure_delay_minutes < 15 and is_cancelled = false then 1 else 0 end) as on_time_departures,
        sum(case when departure_delay_minutes >= 15 and is_cancelled = false then 1 else 0 end) as delayed_departures
    from {{ ref('fct_flights') }}
    group by departure_airport_id
),

arrivals as (
    select
        arrival_airport_id as airport_id,
        count(flight_id) as total_arrivals,
        sum(case when is_cancelled = true then 1 else 0 end) as cancelled_arrivals,
        round(avg(case when is_cancelled = false then arrival_delay_minutes end), 2) as avg_arrival_delay_minutes,
        sum(case when arrival_delay_minutes < 15 and is_cancelled = false then 1 else 0 end) as on_time_arrivals,
        sum(case when arrival_delay_minutes >= 15 and is_cancelled = false then 1 else 0 end) as delayed_arrivals
    from {{ ref('fct_flights') }}
    group by arrival_airport_id
)

select
    -- grain key --
    d.airport_id,

    -- dimension attributes --
    d.airport_name,
    d.airport_iata_code,
    d.city_name,
    d.country_name,
    d.country_continent,
    d.latitude,
    d.longitude,

    -- departure volumes --
    dep.total_departures,
    dep.cancelled_departures,

    -- arrival volumes --
    arr.total_arrivals,
    arr.cancelled_arrivals,

    -- total traffic --
    coalesce(dep.total_departures, 0) + coalesce(arr.total_arrivals, 0) as total_movements,

    -- departure performance --
    dep.avg_departure_delay_minutes,
    dep.on_time_departures,
    dep.delayed_departures,

    -- arrival performance --
    arr.avg_arrival_delay_minutes,
    arr.on_time_arrivals,
    arr.delayed_arrivals

from {{ ref('dim_airport') }} d
left join departures dep
    on d.airport_id = dep.airport_id
left join arrivals arr
    on d.airport_id = arr.airport_id
