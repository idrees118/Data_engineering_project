# 0003. At-least-once delivery with idempotent processing

**Status:** accepted

## Context
Exactly-once across a broker, a file lake and a warehouse needs distributed transactions that
this stack does not have. Dropping events is unacceptable for financial facts.

## Decision
* Offsets are committed only after bronze files are durably written (at-least-once).
* Batch file names are derived from the batch's first/last source position, so a replay
  overwrites the same file.
* Silver de-duplicates on `event_id`, so any duplicate that survives upstream is removed.
* `fct_orders` is rebuilt as a table on each run instead of being incremental. Payments and
  status changes arrive after the order and can arrive late, so any order can change on any run.
  At this volume a rebuild is cheaper and far less error-prone than computing the affected
  set of orders.

## Consequences
* Replays, retries and restarts are safe by construction; tests assert this.
* Gold rebuild cost grows with history. The README's scaling section names the migration
  path (incremental merge keyed on touched orders).
