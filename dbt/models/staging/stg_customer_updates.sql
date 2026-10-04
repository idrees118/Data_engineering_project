select
    event_id,
    json_extract_string(payload, '$.customer_id') as customer_id,
    json_extract_string(payload, '$.email')       as email,
    json_extract_string(payload, '$.country')     as country,
    json_extract_string(payload, '$.tier')        as tier,
    event_time                                    as updated_at,
    ingested_at
from {{ ref('stg_events') }}
where event_type = 'customer_updated'
