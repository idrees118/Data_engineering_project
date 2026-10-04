select
    order_date,
    category,
    sum(quantity)                                              as units_sold,
    sum(net_line_amount) filter (where is_recognised_revenue)  as net_revenue
from {{ ref('fct_order_items') }}
group by 1, 2
