--- Enrich airports with geographic attributes --

select

    a.airport_id,
    a.airport_name,
    a.airport_iata_code,
    a.airport_icao_code,
    a.airport_timezone_name,

    a.latitude,
    a.longitude,

    c.city_id,
    c.city_name,
    c.city_timezone,
    c.city_gmt_offset,

    cc.country_id,
    cc.country_name,
    cc.country_iso_code,
    cc.country_continent,
    cc.country_currency_code

from {{ ref ('stg_airports') }} a
left join {{ ref ('stg_cities') }} c
    on a.airport_city_id = c.city_id
left join  {{ ref ('stg_countries') }} cc
    on c.country_id = cc.country_id