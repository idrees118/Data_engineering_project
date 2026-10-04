# Runbook

Operational procedures for ShopStream. Commands assume the repo's virtualenv is active and
`SHOPSTREAM_DATA_DIR` (or `SHOPSTREAM_LAKE_URI` for S3) points at the lake you are operating on.

## What runs, and when

| Job | Schedule | Command | Safe to re-run? |
|---|---|---|---|
| Ingest -> quality gate -> dbt -> report | hourly (`shopstream_pipeline` DAG) | `shopstream ingest && shopstream quality && dbt build && shopstream report` | Yes. Replays overwrite the same bronze files; silver de-duplicates. |
| Bronze compaction | daily (`shopstream_compaction` DAG) | `shopstream compact` | Yes. Idempotent, and heals itself if killed mid-run. |

Runs must not overlap (`max_active_runs=1`): the ingest checkpoint and the DuckDB warehouse file are
single-writer.

## The hourly run failed

Find the failing task, then:

| Task | Meaning | First thing to check |
|---|---|---|
| `ingest_stream_to_bronze` | Could not read the stream or write bronze | Broker reachable? Disk or bucket permissions? Offsets are only committed after a successful write, so nothing is lost: fix the cause and re-run. |
| `bronze_quality_gate` | The stream looks unhealthy. dbt was deliberately **not** run, so the marts still hold the last good state | See the next section. |
| `dbt_build` | A model or test failed | `dbt build` output names the failing test. A failing reconciliation test (`assert_*`) means a real data or logic problem: do not mute it. |

## Quality gate failures

`shopstream quality` prints one line per check.

* **`dead_letter_ratio` FAIL**: too many messages were rejected. Almost always a producer deploy that
  broke the contract. Look at *why*:

  ```sql
  -- duckdb
  select split_part(error, ':', 1) as kind, split_part(error, ':', 2) as field, count(*) as n
  from read_parquet('<lake>/dead_letter/**/*.parquet', hive_partitioning=true)
  where ingest_date >= current_date - 1
  group by 1, 2 order by n desc;
  ```

  Fix the producer. The raw messages are kept in the `raw` column, so after the fix they can be
  re-published to the topic; ingestion treats them as new events (their `event_id` was never
  accepted).
* **`freshness` FAIL** (only if `SHOPSTREAM_MAX_FRESHNESS_HOURS` is set): nothing new has arrived.
  Check the producers and the topic, not this pipeline.
* **`no_null_event_ids` FAIL**: should be impossible, because validation rejects such messages
  first. Treat it as a bug in ingestion.
* **`bronze_not_empty` FAIL**: wrong lake location (`SHOPSTREAM_LAKE_URI` / `SHOPSTREAM_DATA_DIR`)
  or nothing has ever been ingested.

## Replaying or backfilling

Bronze is append-only and replays are idempotent, so reprocessing is cheap and safe.

* **Re-read the stream from the start (file transport):** delete `data/checkpoints/ingest.json`
  and run `shopstream ingest`. Batch file names are deterministic, so existing batches are
  overwritten, not duplicated.
* **Re-read a Kafka/Redpanda topic:** reset the consumer group's offsets (for Redpanda:
  `rpk group seek shopstream-ingest --to-start --topics shopstream.events`) with the ingest job
  stopped, then run it. Duplicates that reach bronze through a replay beyond the in-memory window
  are removed by silver.
* **Rebuild silver and gold from bronze:** `dbt build --full-refresh`. Needed after changing a
  model's columns (for example `fct_orders`), never for routine runs.
* **Late or corrected events** need no action: silver loads by ingestion time, and gold recomputes
  the orders and customer history they touch.

## Compaction

`shopstream compact` merges the small per-batch files of each partition into one file and removes
duplicates. It skips partitions written to within the last hour (`--min-age SECONDS` to change).
If it is killed between writing the merged file and deleting the originals, that partition
temporarily holds each event twice; silver ignores this and the next run cleans it up. Nothing to do.

## Metrics and alerts

`shopstream ingest` writes `lake/_meta/ingest.prom` in the Prometheus text format after every run
(point node-exporter's textfile collector at that directory). Suggested alerts:

| Alert | Expression | Why |
|---|---|---|
| Ingestion stalled | `time() - shopstream_ingest_last_run_timestamp_seconds > 2 * 3600` | The hourly job stopped succeeding |
| Producers degraded | `shopstream_ingest_dead_letter_ratio > 0.05` | Same threshold as the quality gate; alert before the gate blocks the run |
| Unusual volume | `shopstream_ingest_messages_received_total == 0` | The run succeeded but read nothing |

## Moving the lake to S3 / MinIO

Set `SHOPSTREAM_LAKE_URI=s3://<bucket>/<prefix>` and the `SHOPSTREAM_S3_*` variables (see
`.env.example`), then run dbt with `--target s3`. Local compose example:
`docker-compose.s3.yml`. Existing local bronze files can be copied to the bucket as they are:
the layout is identical.
