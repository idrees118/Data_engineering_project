# 0004. Incremental silver load keyed on ingestion time, with a lookback

**Status:** accepted

## Context
Events carry two timestamps: when they happened (`event_time`) and when we received them
(`ingested_at`). Loading incrementally on `event_time` silently loses late-arriving events,
because their `event_time` is older than the watermark.

## Decision
`stg_events` filters on `ingested_at >= max(ingested_at) - lookback` (default 2h) and uses a
`delete+insert` strategy on `event_id`.

## Consequences
* Late events are loaded on the next run regardless of how late they are.
* The lookback protects against a run that saw only part of a batch (one batch writes one file
  per event type); the overlap is harmless because the merge is idempotent.
* A small amount of work is repeated each run, in exchange for correctness.
