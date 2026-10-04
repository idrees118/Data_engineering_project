select
    event_id,
    json_extract_string(payload, '$.product_id')               as product_id,
    json_extract_string(payload, '$.name')                     as product_name,
    json_extract_string(payload, '$.category')                 as category,
    cast(json_extract(payload, '$.unit_price') as decimal(18, 2)) as unit_price,
    event_time                                                 as updated_at
from {{ ref('stg_events') }}
where event_type = 'product_upserted'
