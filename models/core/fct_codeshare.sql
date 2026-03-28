
-- Model: fct_codeshare
-- Grain: 1 row per flight x marketing airline (a flight can have multiple marketing airlines)
-- Purpose: Expose operating vs. marketing airline pairings per flight,
--          enriched with flight context for BI analysis.

select
    -- identifiers --
    cs.flight_id,
    cs.operating_airline_id,
    cs.marketing_airline_id,

    -- airline names --
    cs.operating_airline_name,
    cs.marketing_airline_name,

    -- flight context --
    fr.flight_date,
    fr.route_key,
    fr.flight_status_clean,

    -- flags --
    (cs.operating_airline_id != cs.marketing_airline_id) as is_true_codeshare

from {{ ref('int_flight_codeshare') }} cs
left join {{ ref('int_flight_routes_metrics') }} fr
    on cs.flight_id = fr.flight_id;