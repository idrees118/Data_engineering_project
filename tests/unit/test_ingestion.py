from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pytest

from shopstream.events import EventType, serialize_event
from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.ingestion.runner import RecentIdFilter
from shopstream.streaming.base import Message
from tests.conftest import make_event


class ListConsumer:
    """Serves a fixed list in batches and records commit calls, like a real consumer would."""

    def __init__(self, values: list[bytes]) -> None:
        self._messages = [Message(f"k{i}", v, f"0:{i}") for i, v in enumerate(values)]
        self._cursor = 0
        self.committed_through = 0

    def poll(self, max_messages: int, timeout_s: float = 1.0) -> list[Message]:
        batch = self._messages[self._cursor : self._cursor + max_messages]
        self._cursor += len(batch)
        return batch

    def commit(self) -> None:
        self.committed_through = self._cursor

    def close(self) -> None: ...


def _event(i: int, event_type: EventType = EventType.ORDER_STATUS_CHANGED) -> bytes:
    payload = (
        {"order_id": f"ord_{i}", "status": "shipped"}
        if event_type == EventType.ORDER_STATUS_CHANGED
        else {"session_id": f"s{i}", "customer_id": None, "product_id": None, "page_type": "home"}
    )
    return serialize_event(
        make_event(event_type, payload, event_id=f"00000000-0000-4000-8000-{i:012d}")
    )


def _rows(root: Path, dataset: str) -> list[tuple]:  # type: ignore[type-arg]
    return duckdb.sql(
        f"select * from read_parquet('{root}/{dataset}/**/*.parquet', hive_partitioning=true)"
    ).fetchall()


def test_valid_events_land_partitioned_by_type_and_ingest_date(tmp_path: Path) -> None:
    values = [_event(1), _event(2, EventType.PAGE_VIEWED)]
    run_ingestion(ListConsumer(values), BronzeWriter(tmp_path))
    files = sorted(p.relative_to(tmp_path).parts[:3] for p in tmp_path.glob("events/**/*.parquet"))
    assert [f[1] for f in files] == ["event_type=order_status_changed", "event_type=page_viewed"]
    assert all(f[2].startswith("ingest_date=") for f in files)


def test_invalid_messages_go_to_the_dead_letter_dataset_with_a_reason(tmp_path: Path) -> None:
    values = [_event(1), b"garbage", _event(2)]
    stats = run_ingestion(ListConsumer(values), BronzeWriter(tmp_path))
    assert (stats.valid, stats.rejected) == (2, 1)
    ((raw, error, position, *_),) = duckdb.sql(
        f"select * from read_parquet('{tmp_path}/dead_letter/**/*.parquet')"
    ).fetchall()
    assert raw == "garbage"
    assert error.startswith("invalid_json")
    assert position == "0:1"


def test_payload_is_stored_as_raw_json(tmp_path: Path) -> None:
    run_ingestion(ListConsumer([_event(1)]), BronzeWriter(tmp_path))
    payload = duckdb.sql(
        f"select payload from read_parquet('{tmp_path}/events/**/*.parquet')"
    ).fetchone()[0]  # type: ignore[index]
    assert json.loads(payload) == {"order_id": "ord_1", "status": "shipped"}


def test_duplicates_are_dropped_and_counted(tmp_path: Path) -> None:
    values = [_event(1), _event(2), _event(1), _event(1)]
    stats = run_ingestion(ListConsumer(values), BronzeWriter(tmp_path))
    assert (stats.valid, stats.duplicates_dropped) == (2, 2)
    assert len(_rows(tmp_path, "events")) == 2


def test_offsets_are_committed_only_after_the_bronze_write(tmp_path: Path) -> None:
    class ExplodingWriter(BronzeWriter):
        def write_events(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise OSError("disk full")

    consumer = ListConsumer([_event(1), _event(2)])
    with pytest.raises(OSError, match="disk full"):
        run_ingestion(consumer, ExplodingWriter(tmp_path))
    assert consumer.committed_through == 0  # nothing acknowledged, so nothing is lost


def test_replaying_a_batch_overwrites_instead_of_duplicating(tmp_path: Path) -> None:
    values = [_event(i) for i in range(5)]
    writer = BronzeWriter(tmp_path)
    run_ingestion(ListConsumer(values), writer)
    run_ingestion(ListConsumer(values), writer)  # crash-recovery replay of the same batch
    assert len(_rows(tmp_path, "events")) == 5


def test_batches_follow_the_configured_size(tmp_path: Path) -> None:
    stats = run_ingestion(
        ListConsumer([_event(i) for i in range(25)]), BronzeWriter(tmp_path), batch_max_messages=10
    )
    assert (stats.batches, stats.received) == (3, 25)


def test_run_log_is_appended(tmp_path: Path) -> None:
    log = tmp_path / "meta" / "runs.jsonl"
    run_ingestion(ListConsumer([_event(1)]), BronzeWriter(tmp_path / "b"), run_log=log)
    run_ingestion(ListConsumer([_event(2)]), BronzeWriter(tmp_path / "b"), run_log=log)
    lines = [json.loads(line) for line in log.read_text().splitlines()]
    assert [line["valid"] for line in lines] == [1, 1]


def test_empty_stream_is_a_no_op(tmp_path: Path) -> None:
    stats = run_ingestion(ListConsumer([]), BronzeWriter(tmp_path))
    assert stats.batches == 0
    assert not list(tmp_path.rglob("*.parquet"))


def test_recent_id_filter_evicts_oldest_ids() -> None:
    f = RecentIdFilter(capacity=2)
    assert [f.seen_before(x) for x in ("a", "b", "a", "c")] == [False, False, True, False]
    assert f.seen_before("b") is False  # evicted when "c" arrived
    assert f.seen_before("c") is True


def test_event_time_is_stored_in_utc(tmp_path: Path) -> None:
    run_ingestion(ListConsumer([_event(1)]), BronzeWriter(tmp_path))
    ((event_time,),) = duckdb.sql(
        f"select event_time from read_parquet('{tmp_path}/events/**/*.parquet')"
    ).fetchall()
    assert event_time == datetime(2025, 1, 5, 12, 0, tzinfo=UTC)
