select
    event_id,
    json_extract_string(payload, '$.order_id')    as order_id,
    json_extract_string(payload, '$.customer_id') as customer_id,
    json_extract_string(payload, '$.currency')    as currency,
    cast(json_extract(payload, '$.discount_amount') as decimal(18, 2)) as discount_amount,
    json_extract(payload, '$.items')              as items_json,
    event_time                                    as placed_at,
    ingested_at
from {{ ref('stg_events') }}
where event_type = 'order_placed'
