
select
    c.flight_id,
    fd.airline_id as operating_airline_id,
    op.airline_name as operating_airline_name,
    c.marketing_airline_id  as marketing_airline_id,
    mk.airline_name as marketing_airline_name

from {{ ref('stg_codeshare') }} c

left join {{ ref('stg_flight_details') }} fd
    on c.flight_id = fd.flight_id

left join {{ ref('stg_airline') }} op
    on fd.airline_id = op.airline_id

left join {{ ref('stg_airline') }} mk
    on c.marketing_airline_id = mk.airline_id ;


