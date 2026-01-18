
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
to_utc_timestamp(
    dep.scheduled_departure_time,
    case
        when dep_geo.airport_timezone_name in ('Unknown', '', null)
            then 'UTC'
        else dep_geo.airport_timezone_name
    end
) as scheduled_departure_time_utc,

to_utc_timestamp(
    arr.scheduled_arrival_time,
    case
        when arr_geo.airport_timezone_name in ('Unknown', '', null)
            then 'UTC'
        else arr_geo.airport_timezone_name
    end
) as scheduled_arrival_time_utc,

-- actual times in utc --
to_utc_timestamp(
    dep.actual_departure_time,
    case
        when dep_geo.airport_timezone_name in ('Unknown', '', null)
            then 'UTC'
        else dep_geo.airport_timezone_name
    end
) as actual_departure_time_utc,

to_utc_timestamp(
    arr.actual_arrival_time,
    case
        when arr_geo.airport_timezone_name in ('Unknown', '', null)
            then 'UTC'
        else arr_geo.airport_timezone_name
    end
) as actual_arrival_time_utc

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

        timestampdiff(
        MINUTE,
        scheduled_departure_time_utc,
        scheduled_arrival_time_utc
    ) as scheduled_duration_minutes,
    timestampdiff(
        MINUTE,
        actual_departure_time_utc,
        actual_arrival_time_utc
    ) as actual_duration_minutes,
    case
        when flight_status = 'landed' then 'no'
        else 'yes'
    end as is_cancelled
from base;


