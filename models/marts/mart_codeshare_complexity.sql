-- Mart: mart_codeshare_complexity
-- Grain: 1 row per operating_airline_id + marketing_airline_id pair
-- Purpose:
--   Expose codeshare partnership complexity — which airlines partner together,
--   how many routes they share, and what proportion of flights are true codeshares.

select
    -- grain keys
    cs.operating_airline_id,
    cs.marketing_airline_id,

    -- dimension attributes
    cs.operating_airline_name,
    cs.marketing_airline_name,

    -- volume metrics
    count(cs.flight_id)                       as total_codeshare_flights,
    count(distinct cs.route_key)              as total_routes_shared,
    sum(cast(cs.is_true_codeshare as int))    as total_true_codeshares,

    -- partnership metrics
    round(
        sum(cast(cs.is_true_codeshare as int)) / count(cs.flight_id),
        2
    ) as codeshare_rate

from {{ ref('fct_codeshare') }} cs
group by
    cs.operating_airline_id,
    cs.marketing_airline_id,
    cs.operating_airline_name,
    cs.marketing_airline_name
