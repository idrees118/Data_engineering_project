select
    event_id,
    json_extract_string(payload, '$.order_id')    as order_id,
    json_extract_string(payload, '$.customer_id') as customer_id,
    json_extract_string(payload, '$.currency')    as currency,
    cast(json_extract(payload, '$.discount_amount') as decimal(18, 2)) as discount_amount,
    json_extract(payload, '$.items')              as items_json,
    schema_version,
    -- v2 fields. v1 events predate them, so the channel is honestly 'unknown', not a guess.
    coalesce(json_extract_string(payload, '$.channel'), 'unknown') as channel,
    json_extract_string(payload, '$.coupon_code') as coupon_code,
    event_time                                    as placed_at,
    ingested_at
from {{ ref('stg_events') }}
where event_type = 'order_placed'
