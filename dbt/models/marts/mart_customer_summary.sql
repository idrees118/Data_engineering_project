{#- Customer value summary. "Today" is the newest order in the data, so results are reproducible. -#}
with as_of as (

    select cast(max(placed_at) as date) as as_of_date from {{ ref('fct_orders') }}

),

per_customer as (

    select
        customer_id,
        count(*)                                             as orders,
        count(*) filter (where net_revenue > 0)              as revenue_orders,
        sum(net_revenue)                                     as lifetime_net_revenue,
        min(order_date)                                      as first_order_date,
        max(order_date)                                      as last_order_date
    from {{ ref('fct_orders') }}
    group by 1

),

scored as (

    select
        c.customer_id,
        c.email,
        c.country,
        c.tier,
        coalesce(p.orders, 0)                                as orders,
        coalesce(p.lifetime_net_revenue, 0)                  as lifetime_net_revenue,
        p.first_order_date,
        p.last_order_date,
        date_diff('day', p.last_order_date, a.as_of_date)    as days_since_last_order,
        percent_rank() over (order by coalesce(p.lifetime_net_revenue, 0)) as revenue_percentile
    from {{ ref('dim_customers') }} c
    cross join as_of a
    left join per_customer p using (customer_id)
    where c.is_current

)

select
    *,
    case
        when orders = 0               then 'prospect'
        when revenue_percentile >= 0.9 then 'high_value'
        when orders >= 2              then 'repeat'
        else 'one_time'
    end as segment
from scored
