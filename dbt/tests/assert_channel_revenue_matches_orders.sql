select
    (select sum(net_revenue) from {{ ref('mart_channel_performance') }}) as channel_total,
    (select sum(net_revenue) from {{ ref('fct_orders') }})               as fact_total
where abs(channel_total - fact_total) > 0.01
