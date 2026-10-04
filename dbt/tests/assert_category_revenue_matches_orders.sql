select
    (select sum(net_revenue) from {{ ref('mart_category_performance') }}) as category_total,
    (select sum(net_revenue) from {{ ref('fct_orders') }})                as fact_total
where abs(category_total - fact_total) > 0.5
