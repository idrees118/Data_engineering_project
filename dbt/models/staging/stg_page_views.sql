select
    event_id,
    json_extract_string(payload, '$.session_id')  as session_id,
    json_extract_string(payload, '$.customer_id') as customer_id,
    json_extract_string(payload, '$.product_id')  as product_id,
    json_extract_string(payload, '$.page_type')   as page_type,
    event_time                                    as viewed_at
from {{ ref('stg_events') }}
where event_type = 'page_viewed'
