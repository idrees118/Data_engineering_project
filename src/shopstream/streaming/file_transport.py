"""A tiny append-only log on disk, behaving like a single-partition topic."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from pathlib import Path

from shopstream.streaming.base import Message

SEGMENT_MAX_LINES = 20_000


class FilePublisher:
    def __init__(self, landing_dir: Path) -> None:
        self._dir = landing_dir
        self._dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(self._dir.glob("segment-*.jsonl"))
        self._segment_no = int(existing[-1].stem.split("-")[1]) if existing else 1
        self._lines_in_segment = self._count_lines(self._segment_path()) if existing else 0
        self._handle = self._segment_path().open("a", encoding="utf-8")

    def _segment_path(self) -> Path:
        return self._dir / f"segment-{self._segment_no:06d}.jsonl"

    @staticmethod
    def _count_lines(path: Path) -> int:
        with path.open("rb") as fh:
            return sum(1 for _ in fh)

    def publish(self, key: str, value: bytes) -> None:
        if self._lines_in_segment >= SEGMENT_MAX_LINES:
            self._handle.close()
            self._segment_no += 1
            self._lines_in_segment = 0
            self._handle = self._segment_path().open("a", encoding="utf-8")
        record = {"key": key, "value": value.decode("utf-8", errors="replace")}
        self._handle.write(json.dumps(record) + "\n")
        self._lines_in_segment += 1

    def flush(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def close(self) -> None:
        self.flush()
        self._handle.close()


class FileConsumer:
    """Reads segments in order and persists its position atomically on `commit`."""

    def __init__(self, landing_dir: Path, checkpoint_path: Path) -> None:
        self._dir = landing_dir
        self._checkpoint = checkpoint_path
        self._committed = self._load()
        self._cursor = dict(self._committed)

    def _load(self) -> dict[str, int]:
        if self._checkpoint.exists():
            data = json.loads(self._checkpoint.read_text())
            return {"segment": int(data["segment"]), "line": int(data["line"])}
        return {"segment": 1, "line": 0}

    def poll(self, max_messages: int, timeout_s: float = 1.0) -> Sequence[Message]:
        out: list[Message] = []
        segment, line = self._cursor["segment"], self._cursor["line"]
        while len(out) < max_messages:
            path = self._dir / f"segment-{segment:06d}.jsonl"
            if not path.exists():
                break
            with path.open("r", encoding="utf-8") as fh:
                for lineno, raw in enumerate(fh):
                    if lineno < line:
                        continue
                    if len(out) >= max_messages or not raw.endswith("\n"):
                        break  # batch full, or the writer is mid-line: pick it up next poll
                    record = json.loads(raw)
                    out.append(
                        Message(
                            record["key"], record["value"].encode("utf-8"), f"{segment}:{lineno}"
                        )
                    )
                    line = lineno + 1
            if len(out) >= max_messages:
                break
            if not (self._dir / f"segment-{segment + 1:06d}.jsonl").exists():
                break
            segment, line = segment + 1, 0  # segment fully consumed, continue in the next one
        self._cursor = {"segment": segment, "line": line}
        return out

    def commit(self) -> None:
        self._checkpoint.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._checkpoint.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._cursor))
        os.replace(tmp, self._checkpoint)  # atomic on POSIX: never a half-written checkpoint
        self._committed = dict(self._cursor)

    def close(self) -> None:
        # Uncommitted progress is deliberately dropped: the next run replays from the checkpoint.
        pass
