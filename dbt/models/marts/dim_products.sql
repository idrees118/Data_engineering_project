-- Type-1 dimension: latest known attributes per product.
select
    product_id,
    product_name,
    category,
    unit_price as current_unit_price,
    updated_at as last_updated_at
from {{ ref('stg_product_updates') }}
qualify row_number() over (partition by product_id order by updated_at desc, event_id desc) = 1
