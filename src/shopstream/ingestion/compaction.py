"""Bronze compaction: merge the many small per-batch files of a partition into one.

Micro-batch ingestion writes one small file per event type per batch. Left alone, a busy day
holds thousands of them and every query pays an open/list cost per file (the "small files
problem", worst on object storage). Compaction rewrites a partition as a single sorted file.

Safety properties:

* **No loss.** The merged file is read back and its row count checked before any original is
  deleted.
* **Crash-safe by design.** If a run dies after writing the merged file but before deleting the
  originals, the partition briefly holds the same events twice. The next run merges *everything*
  in the partition again and de-duplicates by `event_id`, so it heals itself. Readers are safe in
  that window too, because silver de-duplicates on `event_id` regardless.
* **Does not touch hot data.** Partitions that received a file more recently than `min_age` are
  skipped, so compaction never races the ingestion job that is still appending to them.
* **Idempotent.** A compacted partition holds one file, so re-running does nothing.

Row-level `ingested_at` values are preserved, so the incremental dbt models (which load on
ingestion time) do not see compacted data as new.
"""

from __future__ import annotations

import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass

import duckdb
import pyarrow as pa

from shopstream.logging_setup import log_kv
from shopstream.storage import Lake

logger = logging.getLogger(__name__)

# dataset -> column to de-duplicate on (None = keep every row)
DATASETS: dict[str, str | None] = {"events": "event_id", "dead_letter": None}


@dataclass
class CompactionStats:
    partitions_compacted: int = 0
    files_before: int = 0
    files_after: int = 0
    rows_before: int = 0
    rows_after: int = 0

    @property
    def duplicates_removed(self) -> int:
        return self.rows_before - self.rows_after


def compact_bronze(
    lake: Lake,
    *,
    min_age_seconds: float = 3600.0,
    min_files: int = 2,
    now: float | None = None,
) -> CompactionStats:
    now = time.time() if now is None else now
    stats = CompactionStats()
    for dataset, dedupe_key in DATASETS.items():
        for partition, files in _partitions(lake, dataset).items():
            if len(files) < min_files:
                continue
            if any(now - lake.modified_at(f) < min_age_seconds for f in files):
                continue  # still being written to
            _compact_partition(lake, partition, files, dedupe_key, stats)
    return stats


def _partitions(lake: Lake, dataset: str) -> dict[str, list[str]]:
    by_partition: dict[str, list[str]] = defaultdict(list)
    for path in lake.files(dataset):
        by_partition[path.rsplit("/", 1)[0]].append(path)
    return by_partition


def _compact_partition(
    lake: Lake, partition: str, files: list[str], dedupe_key: str | None, stats: CompactionStats
) -> None:
    tables = [lake.read_parquet(f) for f in files]
    merged = pa.concat_tables(tables)
    rows_before = merged.num_rows
    if dedupe_key is not None:
        merged = _dedupe_and_sort(merged, dedupe_key)

    fingerprint = hashlib.sha1("|".join(sorted(files)).encode()).hexdigest()[:12]
    relative = f"{lake.relative(partition)}/compact-{fingerprint}.parquet"
    target = lake.write_parquet(merged, relative)

    # Verify the file we are about to rely on before deleting the ones it replaces.
    written_rows = lake.read_parquet(target).num_rows
    if written_rows != merged.num_rows:
        raise RuntimeError(
            f"compaction verification failed for {relative}: "
            f"wrote {merged.num_rows} rows, read back {written_rows}"
        )

    lake.delete([f for f in files if f != target])
    stats.partitions_compacted += 1
    stats.files_before += len(files)
    stats.files_after += 1
    stats.rows_before += rows_before
    stats.rows_after += merged.num_rows
    log_kv(
        logger,
        "partition compacted",
        partition=lake.relative(partition),
        files=len(files),
        rows=merged.num_rows,
        duplicates_removed=rows_before - merged.num_rows,
    )


def _dedupe_and_sort(table: pa.Table, key: str) -> pa.Table:
    con = duckdb.connect(":memory:")
    try:
        con.register("t", table)
        result = con.sql(
            f"""select * from t
                qualify row_number() over (
                    partition by {key} order by ingested_at, source_position
                ) = 1
                order by event_time, {key}"""
        ).to_arrow_table()
    finally:
        con.close()
    return result.cast(table.schema)
