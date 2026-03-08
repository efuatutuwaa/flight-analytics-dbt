-- Model: dim_airport
-- Grain: 1 row per airport_id
-- Source: int_airport_geography
-- 
-- Description:
--   Airport dimension providing geographic, location, and timezone attributes.
--   Used to enrich flight fact tables with airport context for operational,
--   network, and performance analysis.
--
-- Usage:
--   Joined to fct_flights on departure_airport_id and arrival_airport_id.
--
-- Downstream Consumers:
--   - Airline performance analysis
--   - Airport traffic analysis
--   - Route network visualisation
--   - Map-based dashboards

select 
    airport_id, -- primary key
    city_id,
    country_id,

    -- airport metadata
    airport_name,
    airport_iata_code,
    airport_icao_code,

    -- geography -- 
    latitude,
    longitude,
    airport_timezone_name,

    -- location --
    city_name,
    country_name,
    country_iso_code,
    country_continent
from {{ ref('int_airport_geography') }}