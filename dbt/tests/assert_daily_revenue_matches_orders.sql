-- The headline mart must never drift from the fact table it is built on.
select
    (select sum(net_revenue) from {{ ref('mart_daily_revenue') }}) as mart_total,
    (select sum(net_revenue) from {{ ref('fct_orders') }})         as fact_total
where abs(mart_total - fact_total) > 0.01
