-- A customer can only have one valid version at any instant, and exactly one open-ended one.
select customer_id, valid_from, valid_to, next_valid_from
from (
    select customer_id, valid_from, valid_to,
           lead(valid_from) over (partition by customer_id order by valid_from) as next_valid_from
    from {{ ref('dim_customers') }}
)
where valid_to is distinct from next_valid_from

union all

select customer_id, null, null, null
from {{ ref('dim_customers') }}
group by customer_id
having count(*) filter (where is_current) <> 1
