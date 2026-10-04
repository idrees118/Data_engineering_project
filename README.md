# ShopStream: an event-driven lakehouse platform

[![CI](https://github.com/idrees118/data_engineering_project/actions/workflows/ci.yml/badge.svg)](https://github.com/idrees118/data_engineering_project/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![dbt](https://img.shields.io/badge/dbt-duckdb-orange)
![License](https://img.shields.io/badge/license-MIT-green)

ShopStream takes a stream of e-commerce events (orders, payments, fulfilment, browsing, customer
and catalogue changes), lands it in a partitioned Parquet lake, and models it into tested,
reconciled analytics tables with dbt.

It is built around the problems that make real pipelines hard, not the happy path: **duplicate
events, late arrivals, malformed messages, crashes mid-batch and slowly changing dimensions.** Each
one has a defined behaviour and a test that proves it.

```text
simulator ─▶ Kafka/Redpanda ─▶ validate ─▶ bronze Parquet ─▶ quality gate ─▶ dbt silver ─▶ dbt gold
                                  │                                         (incremental)   (SCD2, facts, marts)
                                  └──▶ dead-letter dataset (with reasons)
                         orchestrated by Airflow · tested in CI · documented with ADRs
```

## What this project demonstrates

| Skill | Where to look |
|---|---|
| Streaming ingestion with explicit offset management | [`ingestion/runner.py`](src/shopstream/ingestion/runner.py), [`streaming/kafka_transport.py`](src/shopstream/streaming/kafka_transport.py) |
| Data contracts and dead-letter handling | [`events.py`](src/shopstream/events.py), [`ingestion/bronze.py`](src/shopstream/ingestion/bronze.py) |
| Medallion architecture on an open format | [`docs/architecture.md`](docs/architecture.md) |
| Idempotent, at-least-once processing | [ADR 0003](docs/adr/0003-at-least-once-and-idempotency.md) |
| Incremental loads that survive late data | [`stg_events.sql`](dbt/models/staging/stg_events.sql), [ADR 0004](docs/adr/0004-incremental-on-ingestion-time.md) |
| Dimensional modelling: SCD2 and point-in-time joins | [`dim_customers.sql`](dbt/models/marts/dim_customers.sql), [`fct_orders.sql`](dbt/models/marts/fct_orders.sql) |
| Data quality at every layer | [`quality/checks.py`](src/shopstream/quality/checks.py), 43 dbt tests, [`dbt/tests`](dbt/tests) |
| Testing against ground truth | [`tests/integration`](tests/integration/test_end_to_end.py) |
| Orchestration, CI, containers | [`orchestration/`](orchestration/dags/shopstream_pipeline.py), [`ci.yml`](.github/workflows/ci.yml), [`docker-compose.yml`](docker-compose.yml) |

## Run it

No Docker needed. Python 3.11+:

```bash
git clone https://github.com/idrees118/data_engineering_project.git && cd data_engineering_project
python -m venv .venv && source .venv/bin/activate
make install
make demo          # simulate -> ingest -> quality gate -> dbt build -> report
```

In a container sandbox, `make demo` took about 12 seconds: it publishes 48,917 messages, ingests
them, builds 16 dbt models and runs 43 data tests. Output, trimmed:

```text
published 48,917 messages  {'clean_events': 48216, 'orders': 2063, 'late': 994, 'duplicates': 461, 'malformed': 240}
ingested IngestStats(received=48917, valid=48216, rejected=240, duplicates_dropped=461, batches=10)
[PASS] dead_letter_ratio: 240 rejected (0.50%, max 5.00%)
Done. PASS=59 WARN=0 ERROR=0 SKIP=0 TOTAL=59

== Revenue by category
   category  units  net_revenue
electronics    758   348,583.22
     sports   1215   203,815.40
       home    777   106,265.34
```

The ingestion numbers are the point. The simulator injected 461 duplicates and 240 corrupt
messages; ingestion dropped exactly 461, rejected exactly 240, and kept exactly the 48,216 clean
events.

### With a real broker (Redpanda, Kafka API)

```bash
make up                                   # Redpanda + console at http://localhost:8080
docker compose run --rm producer          # simulated traffic -> topic (3 partitions)
docker compose run --rm ingest            # topic -> bronze Parquet, manual offset commits
docker compose run --rm transform         # quality gate + dbt build + report
```

### Scheduled

[`orchestration/dags/shopstream_pipeline.py`](orchestration/dags/shopstream_pipeline.py) runs
`ingest -> bronze_quality_gate -> dbt_build -> report` hourly with retries and exponential
backoff. The gate sits before dbt so an unhealthy stream stops the run before it reaches the marts.

## Design decisions worth discussing

| Decision | Why | Record |
|---|---|---|
| Raw JSON payloads in bronze, typed in silver | A parsing mistake is fixed with SQL, never by re-reading the stream | [0002](docs/adr/0002-raw-json-in-bronze.md) |
| Commit offsets only after the durable write; deterministic batch file names | Crashes replay a batch instead of losing it, and a replay overwrites instead of duplicating | [0003](docs/adr/0003-at-least-once-and-idempotency.md) |
| Incremental on *ingestion* time with a lookback | Loading on event time silently drops late events | [0004](docs/adr/0004-incremental-on-ingestion-time.md) |
| SCD2 derived from the event log, joined point-in-time | An order's customer tier is the tier at order time, and a late customer update re-slices history correctly | [`data_model.md`](docs/data_model.md) |
| `fct_orders` rebuilt each run, not incremental | Any order can change when late payments arrive; at this size a rebuild is simpler and safer | [0003](docs/adr/0003-at-least-once-and-idempotency.md) |
| DuckDB + Parquet instead of a warehouse server | Reproducible anywhere, open storage format, easy to swap the engine | [0001](docs/adr/0001-duckdb-and-parquet-lakehouse.md) |
| Transport abstraction (Kafka or file log) | Pipeline logic is testable without a broker | [0005](docs/adr/0005-transport-abstraction.md) |

## How it is tested

```bash
make test-unit    # ~1s, 57 tests: contracts, simulator, transports, ingestion, quality gates, CLI
make test         # adds end-to-end tests that run dbt; ~40s; 95% line coverage
```

The end-to-end test is the one I would point a reviewer at. It does not assert "the SQL ran". It
recomputes revenue in plain Python from the simulator's *clean* events, then checks that the
warehouse matches it exactly, per day, after:

* a **full load** with duplicates deliberately let through to silver,
* a **two-step incremental load** split mid-stream, so orders land before their payments and late
  events and duplicates straddle both runs, followed by a third run that must change nothing,
* an **SCD2 check** that version counts match the real attribute changes.

I also broke the silver de-duplication on purpose to confirm the suite fails (it does, on the dbt
`unique` test and the ground-truth comparison).

CI runs lint (ruff), strict type checking (mypy), the tests on Python 3.11 and 3.12 with a coverage
floor, a from-scratch pipeline run, and a Docker build.

## Repository layout

```text
src/shopstream/
  events.py            event contracts (Pydantic) and parse/validate
  generator/           deterministic traffic simulator with fault injection
  streaming/           Kafka and file-log transports behind one interface
  ingestion/           micro-batch runner, bronze writer, dead letters
  quality/             bronze quality gates
  cli.py, config.py    entry point and typed settings
dbt/
  models/staging/      silver: deduped event log + typed views
  models/marts/        gold: dimensions, facts, business marts
  tests/               singular tests (reconciliation, SCD2 integrity)
orchestration/dags/    Airflow DAG
tests/unit, integration
docs/                  architecture, data model, ADRs
```

## Honest limitations

* **Single machine.** DuckDB is single-writer and the lake is on local disk. [Architecture](docs/architecture.md#what-would-change-at-1000x-the-volume)
  lists what changes at scale: S3 storage, small-file compaction, incremental fact merges.
* **Simulated data.** It is generated, with realistic faults, so the results are checkable against
  ground truth. That is a deliberate trade-off, not a claim about real traffic.
* **The Kafka adapter is tested against a fake client, not a live broker.** The compose stack is
  there to run it for real; a Testcontainers-based CI test is the next step. Docker image builds
  are checked in CI, not locally in every environment.
* **The Airflow DAG** is covered by a test that runs when Airflow is installed (skipped otherwise).

## Roadmap

- [ ] Testcontainers test: producer -> Redpanda -> ingestion -> bronze
- [ ] S3/MinIO as the lake location, plus a compaction job
- [ ] Incremental merge for `fct_orders`
- [ ] Schema registry and a v2 event schema to exercise contract evolution
- [ ] Metrics export (rows, rejects, lag) to Prometheus

## License

MIT
