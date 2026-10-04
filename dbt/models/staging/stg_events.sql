{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
    )
}}

{#-
    Silver entry point: one row per event_id, loaded incrementally on *ingestion* time
    (not event time) so late-arriving events are picked up when they land.
    delete+insert on event_id makes re-reading the lookback window idempotent.
-#}

with bronze as (

    select * from {{ source('bronze', 'events') }}

    {% if is_incremental() %}
    where ingested_at >= (
        select coalesce(max(ingested_at), timestamp '1970-01-01') from {{ this }}
    ) - interval '{{ var("incremental_lookback_hours") }} hours'
    {% endif %}

)

select
    event_id,
    event_type,
    schema_version,
    event_time,
    ingested_at,
    payload::json as payload
from bronze
qualify row_number() over (partition by event_id order by ingested_at, source_position) = 1
