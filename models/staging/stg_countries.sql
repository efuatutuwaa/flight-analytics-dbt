with source as (
    select *
    from {{ source('flights', 'countries') }}
),

cleaned as (
    select 
        country_id,
        country_iso3 as country_iso_code,


        -- descriptive columns --
        trim(country_name) as country_name, 
        trim(capital) as country_capital,
        trim(continent) as country_continent,

        -- demographic columns --
        population as country_population,

        -- currency columns --
        case
            when trim(currency_name) IN ('UNK', 'UNKS','Unknown') then null 
            else trim(currency_name)
        end as country_currency_name,

        case
            when trim(currency_code) IN ('UNK', 'UNKS','Unknown') then null 
            else trim(currency_code)
        end as country_currency_code

    
    from source

)

select * from cleaned
