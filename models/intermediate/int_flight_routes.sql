select
    -- identifiers --
    fd.flight_id,
    fd.departure_airport_id,
    fd.arrival_airport_id,

    -- routes geometry --
    r.route_distance_km,


    -- scheduled times --
    dep.departure_scheduled_time,
    arr.arrival_scheduled_time,
    extract(epoch from (arr.arrival_scheduled_time - dep.departure_scheduled_time )) / 60
        as scheduled_duration_minutes,

    -- actual times --
    dep.departure_actual_time,
    arr.arrival_actual_time,
    extract(epoch from ( arr.arrival_actual_time - dep.departure_actual_time )) / 60
        as actual_duration_minutes,

    -- operational column --
    fd.flight_status,
    case
        when fd.flight_status = 'landed' then 'no'
        else 'yes'
    end as is_cancelled

from {{ ref ('stg_flight_details') }} fd
left join {{ ref ('stg_routes') }} r
    on fd.flight_id = r.flight_id
left join {{ ref ('stg_departures') }} dep
    on fd.flight_id = dep.flight_id
left join {{ ref ('stg_arrivals') }} arr
    on fd.flight_id = arr.flight_id;