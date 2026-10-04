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
* `fct_orders` is **incremental**, but keyed on what changed, not on time alone. Payments and
  status changes arrive after the order and can arrive late, so an order can change long after it
  was loaded. Each run recomputes only the *touched* orders: those with a new order, payment or
  status event ingested after the watermark (`max(last_ingested_at)` minus a lookback), plus every
  order of a customer who has a new `customer_updated` event, because that re-slices the SCD2
  history the point-in-time join reads. `delete+insert` on `order_id` makes re-processing safe.
* The first version of this model was a full rebuild each run (simple and safe at small volume);
  it was made incremental once the end-to-end tests could prove equivalence.

## Consequences
* Replays, retries and restarts are safe by construction; tests assert this.
* Gold cost now scales with the *change volume*, not the history. Dimensions and the small marts
  are still full rebuilds.
* Equivalence is tested, not assumed: after a late payment and after a late customer update,
  only the expected orders change and the result equals `dbt run --full-refresh`. Those tests run
  with a zero lookback; with the default 2h lookback the tiny simulated dataset would be
  reprocessed entirely and prove nothing. Deliberately removing the customer-touch rule makes the
  re-slicing test fail.
* Changing this model's columns requires a one-off `dbt run --full-refresh --select fct_orders`.
