with source as (
    select *
    from {{ source('flights', 'airline') }}
),

cleaned as(
    select
        airline_id, 

        -- identifiers 

        nullif(trim(airline_iata), 'UNK') as airline_iata_code, 
        nullif(trim(airline_icao), 'UNK') as airline_icao_code,

        -- descriptive attributes
        trim(airline_name) as airline_name,
        nullif(trim(airline_type), 'UNK') as airline_type,

        -- geographic attributes
        trim(country_name) as airline_country_name, 
        country_id as airline_country_id, 

        -- fleet info 
        fleet_size as airline_fleet_size,
        fleet_average_age as airline_fleet_average_age,

        --founded year (handling ingestion artefacts)
        case 
            when date_founded = 1900 then null
            else date_founded
        end as airline_year_founded
    from source
)

select * from cleaned