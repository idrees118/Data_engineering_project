with sessions as (

    select
        session_id,
        cast(min(viewed_at) as date)                          as session_date,
        bool_or(page_type = 'product')                        as saw_product,
        bool_or(page_type = 'cart')                           as saw_cart,
        bool_or(page_type = 'checkout')                       as saw_checkout
    from {{ ref('stg_page_views') }}
    group by 1

),

daily_sessions as (

    select
        session_date,
        count(*)                                  as sessions,
        count(*) filter (where saw_product)       as product_sessions,
        count(*) filter (where saw_cart)          as cart_sessions,
        count(*) filter (where saw_checkout)      as checkout_sessions
    from sessions
    group by 1

)

select
    s.session_date,
    s.sessions,
    s.product_sessions,
    s.cart_sessions,
    s.checkout_sessions,
    coalesce(o.orders, 0)                                        as orders,
    round(coalesce(o.orders, 0) * 1.0 / nullif(s.sessions, 0), 4)           as session_to_order_rate,
    round(coalesce(o.orders, 0) * 1.0 / nullif(s.checkout_sessions, 0), 4)  as checkout_to_order_rate
from daily_sessions s
left join (
    select order_date, count(*) as orders from {{ ref('fct_orders') }} group by 1
) o on o.order_date = s.session_date
