select
    order_date,
    channel,
    count(*)                                          as orders,
    count(*) filter (where coupon_code is not null)   as orders_with_coupon,
    sum(net_revenue)                                  as net_revenue
from {{ ref('fct_orders') }}
group by 1, 2
