# 0007. One `Lake` abstraction for local disk and S3-compatible storage

**Status:** accepted

## Context
The lake started on local disk. Production lakes live in object storage, and hard-coding paths
across ingestion, quality checks and (soon) compaction would make that move a rewrite.

## Decision
`shopstream.storage.Lake` owns the location (`SHOPSTREAM_LAKE_URI`: a directory or
`s3://bucket/prefix`). Writes, listing, reads and deletes go through `pyarrow.fs`; DuckDB reads the
same files through `httpfs`, configured by `Lake.configure_duckdb` and by the dbt `s3` target.
Local disk stays the default, so nothing changes for `make demo` or most tests.

* Local writes: hidden temp file + atomic rename. S3 writes: a single PUT, which is already
  all-or-nothing, so the object is written directly.
* Bronze file names stay deterministic, so replays overwrite on S3 exactly as on disk.

## Consequences
* Switching storage is configuration. The compose override `docker-compose.s3.yml` runs the same
  pipeline against MinIO.
* The S3 code path is tested against a mock S3 server (moto) in `tests/integration/test_s3_lake.py`.
  The tests that need DuckDB's `httpfs` extension skip on machines that cannot download it and run
  in CI. The MinIO compose files have been validated for syntax but not run against a live MinIO.
* The ingest checkpoint, run log and metrics file remain on local disk: they describe one worker,
  not the shared lake.
