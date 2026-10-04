-- One row per order line. The order-level discount is spread pro rata over the lines so that
-- category-level revenue reconciles exactly with order-level revenue.
select
    i.order_id,
    i.product_id,
    p.category,
    o.customer_id,
    o.order_date,
    o.order_status,
    i.quantity,
    i.unit_price,
    i.quantity * i.unit_price                                           as line_amount,
    round(
        i.quantity * i.unit_price
        - o.discount_amount * (i.quantity * i.unit_price) / o.gross_amount, 4
    )                                                                   as net_line_amount,
    o.net_revenue > 0                                                   as is_recognised_revenue
from {{ ref('stg_order_items') }} i
join {{ ref('fct_orders') }} o using (order_id)
left join {{ ref('dim_products') }} p using (product_id)
