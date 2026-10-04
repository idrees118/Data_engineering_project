# 0006. Additive schema evolution with per-type versions

**Status:** accepted

## Context
Producers change. `order_placed` needs a sales channel and a coupon code, but v1 events are still
in flight and already stored. Breaking either side (rejecting old events, or failing on new
fields) loses data or stops the pipeline.

## Decision
* The envelope carries `schema_version`; the payload model is chosen by `(event_type, version)`
  in one registry (`VERSIONED_PAYLOAD_MODELS`).
* Versions are **additive**: v2 only adds optional fields with defaults, so v1 payloads stay valid.
  Payload models forbid unknown fields, so a v1 event carrying v2 fields is rejected rather than
  silently reinterpreted.
* A version that does not exist for a type goes to the dead-letter dataset with a clear reason.
* Bronze stores the raw payload (ADR 0002), so nothing changes there. Silver reads new fields
  with explicit defaults; orders from before v2 get channel `unknown`, never a made-up value.

## Consequences
* Rolling a producer forward needs no coordinated deploy: old and new events interleave.
* The simulator switches to v2 half way through its window, so every end-to-end run proves
  mixed-version correctness (order counts per channel match ground truth exactly).
* Breaking changes (renames, type changes) are deliberately out of scope: they need a new event
  type or a schema registry with compatibility checks (roadmap).
