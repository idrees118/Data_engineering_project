select
    event_id,
    json_extract_string(payload, '$.order_id') as order_id,
    json_extract_string(payload, '$.status')   as status,
    event_time                                 as changed_at
from {{ ref('stg_events') }}
where event_type = 'order_status_changed'
