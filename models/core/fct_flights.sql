
with flight_keys as (

    select 
        flight_id, 

         -- airline keys --
        airline_id
        
       

    from {{ ref ('int_flight_airline')}}
),

route_keys as (

    select 

        --- join helper --
        flight_id,

        -- route identifiers --
        departure_airport_id, 
        arrival_airport_id,

        -- route events -- 
        scheduled_departure_time_utc,
        scheduled_arrival_time_utc,
        actual_departure_time_utc,
        actual_arrival_time_utc,


        -- operational events --
        flight_status,
        is_cancelled,

        scheduled_duration_minutes,
        actual_duration_minutes,

        -- route level attributes --
        route_distance_km

    from {{ ref ('int_flight_routes')}}

)

select 
        fk.flight_id,

        -- airline keys --
        fk.airline_id as operating_airline_id,

        -- route identifiers --
        rk.departure_airport_id,
        rk.arrival_airport_id,

        -- timestamps ---

        rk.scheduled_departure_time_utc,
        rk.scheduled_arrival_time_utc,
        rk.actual_departure_time_utc,
        rk.actual_arrival_time_utc,

        -- status --
        rk.flight_status,
        rk.is_cancelled,


        -- metrics --
        rk.scheduled_duration_minutes,
        rk.actual_duration_minutes,
        rk.route_distance_km

    from flight_keys fk
    left join route_keys rk 
        on fk.flight_id = rk.flight_id




