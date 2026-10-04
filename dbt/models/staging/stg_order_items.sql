with exploded as (

    select
        order_id,
        unnest(from_json(
            items_json,
            '[{"product_id":"VARCHAR","quantity":"INTEGER","unit_price":"DOUBLE"}]'
        )) as item
    from {{ ref('stg_orders') }}

)

select
    order_id,
    item.product_id                           as product_id,
    item.quantity                             as quantity,
    cast(item.unit_price as decimal(18, 2))   as unit_price
from exploded
