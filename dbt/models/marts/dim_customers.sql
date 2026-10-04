{#-
    Type-2 slowly changing dimension built directly from the customer event log.
    A row is only created when a tracked attribute actually changes, and valid_to is the
    start of the next version (half-open interval: valid_from <= t < valid_to).
    Because it is derived from the full history on every run, a late-arriving update
    automatically re-slices the affected customer's versions.
-#}

with updates as (

    select
        *,
        lag(country) over w as prev_country,
        lag(tier)    over w as prev_tier
    from {{ ref('stg_customer_updates') }}
    window w as (partition by customer_id order by updated_at, event_id)

),

changes as (

    select * from updates
    where prev_country is null
       or country is distinct from prev_country
       or tier    is distinct from prev_tier

)

select
    md5(customer_id || '|' || cast(updated_at as varchar)) as customer_sk,
    customer_id,
    email,
    country,
    tier,
    updated_at                                                         as valid_from,
    lead(updated_at) over (partition by customer_id order by updated_at, event_id) as valid_to,
    lead(updated_at) over (partition by customer_id order by updated_at, event_id) is null
                                                                       as is_current
from changes
