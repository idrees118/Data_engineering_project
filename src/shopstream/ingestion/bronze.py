"""Bronze layer writer: immutable, append-only Parquet files partitioned Hive-style.

    bronze/events/event_type=order_placed/ingest_date=2025-01-03/batch-<id>.parquet
    bronze/dead_letter/ingest_date=2025-01-03/batch-<id>.parquet

The payload stays as raw JSON text. Parsing and typing belong to the silver layer (dbt), so a
change in how we interpret an event never requires re-ingesting from the stream.
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from shopstream.events import Envelope
from shopstream.storage import Lake
from shopstream.streaming.base import Message

EVENTS_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("schema_version", pa.int32()),
        ("event_time", pa.timestamp("us", tz="UTC")),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("payload", pa.string()),
        ("source_position", pa.string()),
        ("batch_id", pa.string()),
    ]
)

DEAD_LETTER_SCHEMA = pa.schema(
    [
        ("raw", pa.string()),
        ("error", pa.string()),
        ("source_position", pa.string()),
        ("ingested_at", pa.timestamp("us", tz="UTC")),
        ("batch_id", pa.string()),
    ]
)


@dataclass(frozen=True)
class ValidEvent:
    envelope: Envelope
    payload_json: str
    message: Message


@dataclass(frozen=True)
class RejectedMessage:
    message: Message
    error: str


def make_batch_id(messages: Sequence[Message]) -> str:
    """Deterministic: replaying the same messages overwrites the same files instead of adding."""
    fingerprint = f"{messages[0].position}|{messages[-1].position}|{len(messages)}"
    return hashlib.sha1(fingerprint.encode()).hexdigest()[:12]


class BronzeWriter:
    def __init__(self, root: Path | Lake) -> None:
        self.lake = root if isinstance(root, Lake) else Lake.local(root)

    def write_events(
        self, events: Sequence[ValidEvent], batch_id: str, ingested_at: datetime | None = None
    ) -> list[str]:
        if not events:
            return []
        now = ingested_at or datetime.now(UTC)
        by_type: dict[str, list[ValidEvent]] = defaultdict(list)
        for ev in events:
            by_type[ev.envelope.event_type.value].append(ev)

        written = []
        for event_type, group in sorted(by_type.items()):
            table = pa.table(
                {
                    "event_id": [e.envelope.event_id for e in group],
                    "event_type": [event_type] * len(group),
                    "schema_version": pa.array(
                        [e.envelope.schema_version for e in group], pa.int32()
                    ),
                    "event_time": pa.array(
                        [e.envelope.event_time for e in group], pa.timestamp("us", tz="UTC")
                    ),
                    "ingested_at": pa.array([now] * len(group), pa.timestamp("us", tz="UTC")),
                    "payload": [e.payload_json for e in group],
                    "source_position": [e.message.position for e in group],
                    "batch_id": [batch_id] * len(group),
                },
                schema=EVENTS_SCHEMA,
            )
            partition = f"event_type={event_type}/ingest_date={now:%Y-%m-%d}"
            relative = f"events/{partition}/batch-{batch_id}.parquet"
            written.append(self.lake.write_parquet(table, relative))
        return written

    def write_dead_letters(
        self,
        rejected: Sequence[RejectedMessage],
        batch_id: str,
        ingested_at: datetime | None = None,
    ) -> str | None:
        if not rejected:
            return None
        now = ingested_at or datetime.now(UTC)
        table = pa.table(
            {
                "raw": [r.message.value.decode("utf-8", errors="replace") for r in rejected],
                "error": [r.error for r in rejected],
                "source_position": [r.message.position for r in rejected],
                "ingested_at": pa.array([now] * len(rejected), pa.timestamp("us", tz="UTC")),
                "batch_id": [batch_id] * len(rejected),
            },
            schema=DEAD_LETTER_SCHEMA,
        )
        relative = f"dead_letter/ingest_date={now:%Y-%m-%d}/batch-{batch_id}.parquet"
        return self.lake.write_parquet(table, relative)
