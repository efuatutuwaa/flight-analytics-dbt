
with source as (
    select *
    from {{ source('flights', 'flight_details') }}
),

cleaned as (
    select
        flight_id, 
        airline_id,
        departure_airport_id, 
        arrival_airport_id,
        flight_date, 
        -- date_trunc('day', flight_date) as flight_day,
        flight_status,
        flight_number, 
        case 
            when trim(flight_icao) in ('UNK', 'UNKS') then null 
            else trim(flight_icao)
        end as flight_icao,

        case 
            when trim(flight_iata) in ('UNK', 'UNKS') then null 
            else trim(flight_iata)
        end as flight_iata 

    from source
)

select * from cleaned