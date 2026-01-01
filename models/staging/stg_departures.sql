with source as (
    select *
    from {{ source('flights', 'departure') }}
), 

cleaned as (
    select 
        departure_id, 

        -- identifiers 
        flight_id, 
        departure_airport_id,
        case
            when trim(airport_iata) in ('UNK', 'UNKS') then null 
            else trim(airport_iata)
        end as departure_airport_iata_code,

        case when trim(airport_icao) in ('UNK', 'UNKS') then null 
            else trim(airport_icao)
        end as departure_airport_icao_code,

        -- descriptive columns 
        trim(airport_name) as departure_airport_name,
        trim(terminal) as departure_terminal,
        trim(gate) as departure_gate,

        -- geographical columns
        timezone as departure_timezone, 

        -- time columns
        scheduled as scheduled_departure_time,
        estimated as estimated_departure_time,
        actual as actual_departure_time,
        estimated_runway as estimated_departure_runway_time,
        actual_runway as actual_departure_runway_time,

        -- operational columns
        delay as departure_delay_minutes

    from source

)

select * from cleaned