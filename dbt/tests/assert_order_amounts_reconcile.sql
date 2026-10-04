-- Line-level revenue must add back up to the order total (allowing for rounding of the
-- pro-rata discount allocation).
select o.order_id, o.net_amount, sum(i.net_line_amount) as line_total
from {{ ref('fct_orders') }} o
join {{ ref('fct_order_items') }} i using (order_id)
group by o.order_id, o.net_amount
having abs(o.net_amount - sum(i.net_line_amount)) > 0.01
