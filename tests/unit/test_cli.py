from __future__ import annotations

from pathlib import Path

import pytest

from shopstream.cli import main
from shopstream.config import Settings, get_settings
from shopstream.streaming import build_consumer, build_publisher
from shopstream.streaming.file_transport import FileConsumer, FilePublisher


@pytest.fixture
def data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("SHOPSTREAM_DATA_DIR", str(tmp_path))
    get_settings.cache_clear()
    yield tmp_path
    get_settings.cache_clear()


SMALL = ["--days", "2", "--orders-per-day", "15", "--customers", "40"]


def test_generate_ingest_quality_flow(data_dir: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["generate", *SMALL]) == 0
    assert list((data_dir / "landing").glob("segment-*.jsonl"))

    assert main(["ingest"]) == 0
    assert list((data_dir / "lake" / "bronze" / "events").rglob("*.parquet"))
    assert (data_dir / "lake" / "_meta" / "ingest_runs.jsonl").exists()

    assert main(["quality"]) == 0
    assert "[PASS] dead_letter_ratio" in capsys.readouterr().out


def test_second_ingest_with_no_new_data_is_a_no_op(data_dir: Path) -> None:
    main(["generate", *SMALL])
    main(["ingest"])
    before = sorted((data_dir / "lake").rglob("*.parquet"))
    assert main(["ingest"]) == 0
    assert sorted((data_dir / "lake").rglob("*.parquet")) == before


def test_quality_gate_exits_non_zero_when_bronze_is_empty(data_dir: Path) -> None:
    assert main(["quality"]) == 1


def test_report_without_a_warehouse_explains_what_to_do(
    data_dir: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["report"]) == 1
    assert "dbt-build" in capsys.readouterr().err


def test_transport_factory_selects_implementation(tmp_path: Path) -> None:
    file_settings = Settings(data_dir=tmp_path, transport="file")
    publisher = build_publisher(file_settings)
    assert isinstance(publisher, FilePublisher)
    publisher.close()  # type: ignore[attr-defined]
    assert isinstance(build_consumer(file_settings), FileConsumer)


def test_settings_validate_their_bounds() -> None:
    with pytest.raises(ValueError, match="max_dead_letter_ratio"):
        Settings(max_dead_letter_ratio=1.5)
