from __future__ import annotations

from pathlib import Path

from shopstream.streaming import file_transport
from shopstream.streaming.file_transport import FileConsumer, FilePublisher


def _publish(landing: Path, n: int, start: int = 0) -> None:
    pub = FilePublisher(landing)
    for i in range(start, start + n):
        pub.publish(f"k{i}", f'{{"n": {i}}}'.encode())
    pub.close()


def test_consumer_reads_everything_in_order(tmp_path: Path) -> None:
    _publish(tmp_path / "land", 25)
    consumer = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    got = list(consumer.poll(100))
    assert [m.key for m in got] == [f"k{i}" for i in range(25)]
    assert consumer.poll(100) == []


def test_poll_respects_batch_size(tmp_path: Path) -> None:
    _publish(tmp_path / "land", 25)
    consumer = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    assert [len(consumer.poll(10)) for _ in range(4)] == [10, 10, 5, 0]


def test_committed_position_survives_restart(tmp_path: Path) -> None:
    _publish(tmp_path / "land", 10)
    first = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    first.poll(4)
    first.commit()
    resumed = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    assert [m.key for m in resumed.poll(100)] == [f"k{i}" for i in range(4, 10)]


def test_uncommitted_messages_are_replayed_after_a_crash(tmp_path: Path) -> None:
    _publish(tmp_path / "land", 10)
    crashed = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    crashed.poll(6)  # no commit: the process dies here
    restarted = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    assert len(restarted.poll(100)) == 10


def test_new_data_is_picked_up_after_draining(tmp_path: Path) -> None:
    landing = tmp_path / "land"
    _publish(landing, 3)
    consumer = FileConsumer(landing, tmp_path / "cp.json")
    consumer.poll(100)
    consumer.commit()
    _publish(landing, 2, start=3)  # a new publisher appends to the same segment
    assert [m.key for m in consumer.poll(100)] == ["k3", "k4"]


def test_segments_roll_over_and_reads_span_them(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(file_transport, "SEGMENT_MAX_LINES", 4)
    _publish(tmp_path / "land", 10)
    assert len(list((tmp_path / "land").glob("segment-*.jsonl"))) == 3
    consumer = FileConsumer(tmp_path / "land", tmp_path / "cp.json")
    assert [m.key for m in consumer.poll(100)] == [f"k{i}" for i in range(10)]


def test_half_written_line_is_not_consumed(tmp_path: Path) -> None:
    landing = tmp_path / "land"
    _publish(landing, 2)
    with (landing / "segment-000001.jsonl").open("a") as fh:
        fh.write('{"key": "k2", "val')  # writer is mid-line
    consumer = FileConsumer(landing, tmp_path / "cp.json")
    assert len(consumer.poll(100)) == 2
