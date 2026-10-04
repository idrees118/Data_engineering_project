-- Guards against clock-skew bugs in producers: event_time may not exceed ingestion time.
select event_id, event_time, ingested_at
from {{ ref('stg_events') }}
where event_time > ingested_at
