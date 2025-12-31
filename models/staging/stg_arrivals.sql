with source as (
    select *
    from {{ source('flights', 'arrivals') }}
), 

cleaned as (
    select 
        arrival_id, 

        -- identifiers 
        flight_id, 
        arrival_airport_id,
        case
            when trim(airport_iata) in ('UNK', 'UNKS') then null 
            else trim(airport_iata)
        end as arrival_airport_iata_code,

        case when trim(airport_icao) in ('UNK', 'UNKS') then null 
            else trim(airport_icao)
        end as arrival_airport_icao_code,

        -- descriptive columns 
        trim(airport_name) as arrival_airport_name,
        trim(terminal) as arrival_terminal,
        trim(gate) as arrival_gate,
        trim(baggage) as arrival_baggage_claim,

        -- geographical columns
        timezone as arrival_timezone, 

        -- time columns
        scheduled as scheduled_arrival_time,
        estimated as estimated_arrival_time,
        actual as actual_arrival_time,
        estimated_runway as estimated_arrival_runway_time,
        actual_runway as actual_arrival_runway_time,

        -- operational columns
        delay as arrival_delay_minutes

    from source

)

select * from cleaned