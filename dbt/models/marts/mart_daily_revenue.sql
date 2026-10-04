select
    order_date,
    count(*)                                        as orders,
    count(*) filter (where paid_at is not null)     as paid_orders,
    sum(gross_amount)  filter (where net_revenue > 0) as gross_revenue,
    sum(discount_amount) filter (where net_revenue > 0) as discounts,
    sum(refunded_amount)                            as refunds,
    sum(net_revenue)                                as net_revenue,
    round(sum(net_revenue) / nullif(count(*) filter (where net_revenue > 0), 0), 2) as aov
from {{ ref('fct_orders') }}
group by 1
