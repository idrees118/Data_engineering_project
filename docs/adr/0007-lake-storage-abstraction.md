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
  pipeline against a local S3-compatible server.
* The S3 code path is tested twice: against a mock S3 server in-process (moto) for fast feedback,
  and against a real S3-compatible server in a container (`tests/integration/test_docker_services.py`),
  which runs in CI. Both cover ingestion, the quality gates and a full dbt build from the bucket.
* MinIO stopped publishing community container images, so the local stack and the container tests
  use Adobe's S3Mock. The code only needs an S3-compatible endpoint: a MinIO build, Ceph or AWS S3
  work by changing `SHOPSTREAM_S3_*`. Those have not been tested here.
* The ingest checkpoint, run log and metrics file remain on local disk: they describe one worker,
  not the shared lake.
