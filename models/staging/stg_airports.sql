
with source as (
    select *
    from {{ source('flights', 'airports') }}
),

cleaned as (
    select 
        airport_id, 

        -- identifiers
       case 
            when trim(airport_iata) in ('UNK', 'UNKS') then null 
            else trim(airport_iata)
        end as airport_iata_code, 

        case 
            when trim(airport_icao) in ('UNK', 'UNKS') then null 
            else trim(airport_icao)
        end as airport_icao_code, 

        -- descriptive columns 

        trim(airport_name) as airport_name, 

        -- geographical columns 
        airport_city_id, 
        trim(timezone_name) as airport_timezone_name, 
        trim(gmt_offset) as airport_gmt_offset,
        latitude, 
        longitude
from source

)

select * from cleaned