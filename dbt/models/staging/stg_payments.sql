select
    event_id,
    json_extract_string(payload, '$.payment_id') as payment_id,
    json_extract_string(payload, '$.order_id')   as order_id,
    cast(json_extract(payload, '$.amount') as decimal(18, 2)) as amount,
    json_extract_string(payload, '$.method')     as method,
    json_extract_string(payload, '$.status')     as status,
    event_time                                   as processed_at
from {{ ref('stg_events') }}
where event_type = 'payment_processed'
