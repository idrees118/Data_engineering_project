{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
    )
}}

{#-
    One row per order with its full lifecycle folded in.

    Incremental: only orders *touched* since the last run are recomputed. An order is touched when
    a new order, payment or status event for it was ingested, or when its customer has a new
    customer_updated event (that re-slices the SCD2 history the order joins to). The cutoff is the
    newest `last_ingested_at` already in this table minus a lookback; the comparison is strict
    because the lookback is what provides the overlap, and delete+insert on order_id makes
    re-processing that overlap idempotent. See docs/adr/0003.
-#}

-- depends_on: {{ ref('stg_customer_updates') }}
{% if is_incremental() %}
{% set cutoff %}(
    select coalesce(max(last_ingested_at), timestamp '1970-01-01') from {{ this }}
) - interval '{{ var("incremental_lookback_hours") }} hours'{% endset %}
{% endif %}

with

{% if is_incremental() %}
touched as (

    select order_id from {{ ref('stg_orders') }} where ingested_at > {{ cutoff }}
    union
    select order_id from {{ ref('stg_payments') }} where ingested_at > {{ cutoff }}
    union
    select order_id from {{ ref('stg_order_status_changes') }} where ingested_at > {{ cutoff }}
    union
    select o.order_id
    from {{ ref('stg_orders') }} o
    join {{ ref('stg_customer_updates') }} c using (customer_id)
    where c.ingested_at > {{ cutoff }}

),
{% endif %}

items as (

    select
        order_id,
        count(*)                          as line_count,
        sum(quantity)                     as units,
        sum(quantity * unit_price)        as gross_amount
    from {{ ref('stg_order_items') }}
    {% if is_incremental() %}where order_id in (select order_id from touched){% endif %}
    group by 1

),

payments as (

    select
        order_id,
        count(*)                                           as payment_attempts,
        count(*) filter (where status = 'failed')          as failed_payment_attempts,
        min(processed_at) filter (where status = 'succeeded') as paid_at,
        max(method)  filter (where status = 'succeeded')   as payment_method,
        max(ingested_at)                                   as last_ingested_at
    from {{ ref('stg_payments') }}
    {% if is_incremental() %}where order_id in (select order_id from touched){% endif %}
    group by 1

),

lifecycle as (

    select
        order_id,
        min(changed_at) filter (where status = 'shipped')   as shipped_at,
        min(changed_at) filter (where status = 'delivered') as delivered_at,
        min(changed_at) filter (where status = 'cancelled') as cancelled_at,
        min(changed_at) filter (where status = 'refunded')  as refunded_at,
        max(ingested_at)                                    as last_ingested_at
    from {{ ref('stg_order_status_changes') }}
    {% if is_incremental() %}where order_id in (select order_id from touched){% endif %}
    group by 1

),

orders as (

    select
        o.order_id,
        o.customer_id,
        o.placed_at,
        cast(o.placed_at as date)                                   as order_date,
        o.discount_amount,
        o.channel,
        o.coupon_code,
        i.line_count,
        i.units,
        i.gross_amount,
        i.gross_amount - o.discount_amount                          as net_amount,
        coalesce(p.payment_attempts, 0)                             as payment_attempts,
        coalesce(p.failed_payment_attempts, 0)                      as failed_payment_attempts,
        p.paid_at,
        p.payment_method,
        l.shipped_at,
        l.delivered_at,
        l.cancelled_at,
        l.refunded_at,
        greatest(
            o.ingested_at,
            coalesce(p.last_ingested_at, o.ingested_at),
            coalesce(l.last_ingested_at, o.ingested_at)
        ) as last_ingested_at,
        o.ingested_at - o.placed_at
            > interval '{{ var("late_arrival_threshold_hours") }} hours' as arrived_late
    from {{ ref('stg_orders') }} o
    left join items i using (order_id)
    left join payments p using (order_id)
    left join lifecycle l using (order_id)
    {% if is_incremental() %}where o.order_id in (select order_id from touched){% endif %}

)

select
    o.order_id,
    c.customer_sk,
    o.customer_id,
    -- point-in-time: the customer's attributes *as they were when the order was placed*
    c.tier    as customer_tier_at_order,
    c.country as customer_country_at_order,
    o.placed_at,
    o.order_date,
    o.channel,
    o.coupon_code,
    o.line_count,
    o.units,
    o.gross_amount,
    o.discount_amount,
    o.net_amount,
    o.payment_attempts,
    o.failed_payment_attempts,
    o.payment_method,
    o.paid_at,
    o.shipped_at,
    o.delivered_at,
    o.cancelled_at,
    o.refunded_at,
    case
        when o.refunded_at   is not null then 'refunded'
        when o.cancelled_at  is not null then 'cancelled'
        when o.delivered_at  is not null then 'delivered'
        when o.shipped_at    is not null then 'shipped'
        when o.paid_at       is not null then 'paid'
        when o.failed_payment_attempts > 0 then 'payment_failed'
        else 'pending_payment'
    end as order_status,
    -- revenue is recognised once paid and stays recognised until a refund is issued
    case when o.paid_at is not null and o.refunded_at is null then o.net_amount else 0 end
        as net_revenue,
    case when o.refunded_at is not null and o.paid_at is not null then o.net_amount else 0 end
        as refunded_amount,
    o.arrived_late,
    o.last_ingested_at
from orders o
left join {{ ref('dim_customers') }} c
    on  c.customer_id = o.customer_id
    and o.placed_at >= c.valid_from
    and (o.placed_at < c.valid_to or c.valid_to is null)
