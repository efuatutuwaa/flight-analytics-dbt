
with base as (
    select
    -- identifiers --
    fd.flight_id,
    fd.departure_airport_id,
    fd.arrival_airport_id,
    fd.flight_status,
    dep_geo.airport_timezone_name as departure_airport_timezone,
    arr_geo.airport_timezone_name as arrival_airport_timezone,

    -- routes geometry --
    r.route_distance_km,


    -- scheduled times in local time --
    dep.scheduled_departure_time as scheduled_departure_time_local,
    arr.scheduled_arrival_time as scheduled_arrival_time_local,

    -- actual times in local time --

    dep.actual_departure_time as actual_departure_time_local,
    arr.actual_arrival_time as actual_arrival_time_local,

    -- scheduled times in utc --
    dep.scheduled_departure_time at time zone dep_geo.airport_timezone_name
        as scheduled_departure_time_utc,
    arr.scheduled_arrival_time at time zone arr_geo.airport_timezone_name
        as scheduled_arrival_time_utc,

    -- actual times in utc --
     dep.actual_departure_time at time zone dep_geo.airport_timezone_name
        as actual_departure_time_utc,
    arr.actual_arrival_time at time zone arr_geo.airport_timezone_name
        as actual_arrival_time_utc


from {{ ref ('stg_flight_details') }} fd
left join {{ ref ('stg_routes') }} r
    on fd.flight_id = r.flight_id
left join {{ ref ('stg_departures') }} dep
    on fd.flight_id = dep.flight_id
left join {{ ref ('stg_arrivals') }} arr
    on fd.flight_id = arr.flight_id
left join {{ ref ('int_airport_geography')  }} dep_geo
    on fd.departure_airport_id = dep_geo.airport_id
left join {{ ref ('int_airport_geography')  }} arr_geo
    on fd.arrival_airport_id = arr_geo.airport_id

)

select 
    *, 

    extract(epoch from (scheduled_arrival_time_utc - scheduled_departure_time_utc)) / 60
        as scheduled_duration_minutes,
    extract(epoch from (actual_arrival_time_utc - actual_departure_time_utc)) / 60
        as actual_duration_minutes,
    case
        when flight_status = 'landed' then 'no'
        else 'yes'
    end as is_cancelled
from base;

