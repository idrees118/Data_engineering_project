from __future__ import annotations

from pathlib import Path

import pytest

from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.metrics import render, write_textfile
from tests.unit.test_ingestion import ListConsumer, _event


def test_render_follows_the_exposition_format() -> None:
    text = render({"events_valid_total": 5, "dead_letter_ratio": 0.25})
    assert "# TYPE shopstream_ingest_events_valid_total counter" in text
    assert "shopstream_ingest_events_valid_total 5\n" in text
    assert "shopstream_ingest_dead_letter_ratio 0.25\n" in text
    assert text.endswith("\n")


def test_unknown_metric_names_fail_loudly() -> None:
    with pytest.raises(KeyError, match="nope"):
        render({"nope": 1})


def test_textfile_is_replaced_without_leaving_temp_files(tmp_path: Path) -> None:
    target = tmp_path / "m" / "ingest.prom"
    write_textfile(target, {"batches_total": 1})
    write_textfile(target, {"batches_total": 2})
    assert "batches_total 2" in target.read_text()
    assert [p.name for p in target.parent.iterdir()] == ["ingest.prom"]


def test_ingestion_publishes_its_stats(tmp_path: Path) -> None:
    prom = tmp_path / "ingest.prom"
    values = [_event(1), b"junk", _event(1), _event(2)]
    run_ingestion(ListConsumer(values), BronzeWriter(tmp_path / "b"), metrics_path=prom)
    text = prom.read_text()
    assert "shopstream_ingest_messages_received_total 4" in text
    assert "shopstream_ingest_events_rejected_total 1" in text
    assert "shopstream_ingest_duplicates_dropped_total 1" in text
    assert "shopstream_ingest_last_run_timestamp_seconds" in text
