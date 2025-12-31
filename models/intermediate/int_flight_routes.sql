select
    -- identifiers --
    fd.flight_id,
    fd.departure_airport_id,
    fd.arrival_airport_id,

    -- routes geometry --
    r.route_distance_km,


    -- scheduled times --
    dep.scheduled_departure_time,
    arr.scheduled_arrival_time,
    extract(epoch from (arr.scheduled_arrival_time - dep.scheduled_departure_time )) / 60
        as scheduled_duration_minutes,

    -- actual times --
    dep.actual_departure_time,
    arr.actual_arrival_time,
    extract(epoch from ( arr.actual_arrival_time - dep.actual_departure_time )) / 60
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