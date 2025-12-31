with source as (
    select *
    from {{ source('flights', 'routes') }}
),

cleaned as (
    select 
        route_id, 
        flight_id,
        airline_id,
        departure_airport_id, 
        arrival_airport_id,
        distance as route_distance_km,
        duration as route_duration,

        -- time columns 
        scheduled_departure_time,
        scheduled_arrival_time

    from source       

)

select * from cleaned