# 0001. DuckDB + Parquet as the lakehouse engine

**Status:** accepted

## Context
The project must be fully runnable by a reviewer in one command, but still use the structures of
a production lakehouse: open file formats, partitioned storage, layered models, SQL transformations
under version control.

## Decision
Store bronze as Hive-partitioned Parquet (zstd) and run silver/gold transformations with dbt on
DuckDB, which reads the Parquet directly.

## Consequences
* No database server to provision; CI runs the exact same pipeline as a laptop.
* Storage is open and engine-agnostic. The same Parquet can be queried by Spark, Trino or
  Snowflake external tables.
* DuckDB is single-writer, so the orchestrator must serialise runs (`max_active_runs=1`).
* The scale ceiling is one machine. The README states this and lists what changes beyond it.
