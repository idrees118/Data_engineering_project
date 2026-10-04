from __future__ import annotations

from pathlib import Path

import pytest

from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.ingestion.compaction import compact_bronze
from shopstream.storage import Lake
from tests.unit.test_ingestion import ListConsumer, _event


def _ingest(root: Path, ids: range, batch: int = 5, dedup_window: int = 10_000) -> None:
    values = [_event(i) for i in ids]
    run_ingestion(
        ListConsumer(values),
        BronzeWriter(root),
        batch_max_messages=batch,
        dedup_window=dedup_window,
    )


def _event_ids(lake: Lake) -> list[str]:
    ids: list[str] = []
    for f in lake.files("events"):
        ids += lake.read_parquet(f).column("event_id").to_pylist()
    return sorted(ids)


def test_small_files_are_merged_into_one_per_partition(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    _ingest(tmp_path, range(23))  # 5 batches -> 5 small files in one partition
    assert len(lake.files("events")) == 5
    before = _event_ids(lake)

    stats = compact_bronze(lake, min_age_seconds=0)

    assert len(lake.files("events")) == 1
    assert _event_ids(lake) == before, "no event may be lost or invented"
    assert (stats.partitions_compacted, stats.files_before, stats.files_after) == (1, 5, 1)


def test_compaction_is_idempotent(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    _ingest(tmp_path, range(12))
    compact_bronze(lake, min_age_seconds=0)
    files = lake.files("events")
    stats = compact_bronze(lake, min_age_seconds=0)
    assert stats.partitions_compacted == 0
    assert lake.files("events") == files


def test_recent_partitions_are_left_alone(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    _ingest(tmp_path, range(12))
    stats = compact_bronze(lake, min_age_seconds=3600)
    assert stats.partitions_compacted == 0
    assert len(lake.files("events")) == 3


def test_new_batches_are_folded_into_the_existing_compacted_file(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    _ingest(tmp_path, range(0, 10))
    compact_bronze(lake, min_age_seconds=0)
    _ingest(tmp_path, range(10, 20))
    assert len(lake.files("events")) == 3
    compact_bronze(lake, min_age_seconds=0)
    assert len(lake.files("events")) == 1
    assert len(_event_ids(lake)) == 20


def test_duplicates_are_removed_when_merging(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    # a window of 1 lets duplicates through to bronze, as after a restart
    _ingest(tmp_path, range(10), dedup_window=1)
    # batch size 4 gives these batches different positions than the first run, so they are
    # new files rather than a replay that would overwrite the originals
    _ingest(tmp_path, range(5), batch=4, dedup_window=1)
    assert len(_event_ids(lake)) == 15
    stats = compact_bronze(lake, min_age_seconds=0)
    assert len(_event_ids(lake)) == 10
    assert stats.duplicates_removed == 5


def test_crash_between_write_and_delete_heals_on_the_next_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = Lake.local(tmp_path)
    _ingest(tmp_path, range(15))
    expected = _event_ids(lake)

    real_delete = Lake.delete

    def crash(self: Lake, paths: list[str]) -> None:
        raise OSError("process killed before deleting originals")

    monkeypatch.setattr(Lake, "delete", crash)
    with pytest.raises(OSError, match="killed"):
        compact_bronze(lake, min_age_seconds=0)
    assert len(_event_ids(lake)) == 30, "merged file and originals both exist: events doubled"

    monkeypatch.setattr(Lake, "delete", real_delete)
    compact_bronze(lake, min_age_seconds=0)
    assert _event_ids(lake) == expected
    assert len(lake.files("events")) == 1


def test_dead_letters_are_compacted_without_dropping_rows(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    values = [v for i in range(4) for v in (_event(i), b"junk", b"junk")]
    run_ingestion(ListConsumer(values), BronzeWriter(tmp_path), batch_max_messages=3)
    before = len(lake.files("dead_letter"))
    assert before >= 1
    compact_bronze(lake, min_age_seconds=0)
    rows = sum(lake.read_parquet(f).num_rows for f in lake.files("dead_letter"))
    assert rows == 8
    assert len(lake.files("dead_letter")) == 1


def test_row_data_survives_unchanged(tmp_path: Path) -> None:
    lake = Lake.local(tmp_path)
    _ingest(tmp_path, range(9))
    snapshot = {
        r["event_id"]: r for f in lake.files("events") for r in lake.read_parquet(f).to_pylist()
    }
    compact_bronze(lake, min_age_seconds=0)
    after = {
        r["event_id"]: r for f in lake.files("events") for r in lake.read_parquet(f).to_pylist()
    }
    assert after == snapshot  # payload, event_time, ingested_at, batch_id: all identical
