
with source as (
    select *
    from {{ source('flights', 'cities') }}
),

cleaned as (
    select
        city_id, 
        country_id,

        -- descriptive columns --
        trim(city_name) as city_name,

        -- geographical columns --
        case 
            when latitude in (-999.000000) then null 
            else latitude
        end as latitude,

        case 
            when longitude in (-999.000000) then null 
            else longitude
        end as longitude,

        timezone as city_timezone, 
        gmt_offset as city_gmt_offset

    from source

)

select * from cleaned
