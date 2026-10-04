from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from shopstream.events import serialize_event
from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.quality import run_bronze_checks
from shopstream.streaming.base import Message
from tests.conftest import make_event


class _Consumer:
    def __init__(self, values: list[bytes]) -> None:
        self._m = [Message("k", v, f"0:{i}") for i, v in enumerate(values)]

    def poll(self, max_messages: int, timeout_s: float = 1.0) -> list[Message]:
        batch, self._m = self._m[:max_messages], self._m[max_messages:]
        return batch

    def commit(self) -> None: ...

    def close(self) -> None: ...


def _ingest(root: Path, good: int, bad: int) -> None:
    values = [
        serialize_event(make_event(event_id=f"00000000-0000-4000-8000-{i:012d}"))
        for i in range(good)
    ] + [b"junk"] * bad
    run_ingestion(_Consumer(values), BronzeWriter(root))


def _by_name(results):  # type: ignore[no-untyped-def]
    return {r.name: r for r in results}


def test_healthy_bronze_passes(tmp_path: Path) -> None:
    _ingest(tmp_path, good=100, bad=1)
    results = run_bronze_checks(tmp_path, max_dead_letter_ratio=0.05, max_freshness_hours=None)
    assert all(r.passed for r in results)


def test_high_reject_ratio_fails_the_gate(tmp_path: Path) -> None:
    _ingest(tmp_path, good=10, bad=10)
    results = _by_name(
        run_bronze_checks(tmp_path, max_dead_letter_ratio=0.05, max_freshness_hours=None)
    )
    assert not results["dead_letter_ratio"].passed


def test_stale_data_fails_the_freshness_gate(tmp_path: Path) -> None:
    _ingest(tmp_path, good=5, bad=0)  # newest event is 2025-01-05
    now = datetime(2025, 1, 5, 12, 0, tzinfo=UTC) + timedelta(hours=30)
    results = _by_name(
        run_bronze_checks(tmp_path, max_dead_letter_ratio=1, max_freshness_hours=24, now=now)
    )
    assert not results["freshness"].passed
    results = _by_name(
        run_bronze_checks(tmp_path, max_dead_letter_ratio=1, max_freshness_hours=48, now=now)
    )
    assert results["freshness"].passed


def test_empty_bronze_fails(tmp_path: Path) -> None:
    (result,) = run_bronze_checks(tmp_path, max_dead_letter_ratio=1, max_freshness_hours=None)
    assert not result.passed
