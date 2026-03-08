
-- This model defines the dim_airline dimension table, which contains attributes related to airlines.
-- Grain: 1 record per airline, stg_airline is the source table for this model.

select 
    airline_id, -- primary key
    airline_country_id,  

    -- identifier codes 
    airline_iata_code,
    airline_icao_code,

    -- descriptive attributes
    airline_name,
    airline_type,
    airline_country_name,

    -- fleet info 
    airline_fleet_size,
    airline_fleet_average_age,

    -- date attributes
    airline_year_founded
from {{ ref('stg_airline') }}



