"""Micro-batch ingestion: poll -> validate -> de-duplicate -> write bronze -> commit offsets.

Delivery semantics are at-least-once. Offsets are committed only after the batch is durably
written, so a crash replays at most one batch. Replays are harmless: batch file names are
deterministic (same input overwrites the same file) and silver de-duplicates on `event_id`.
"""

from __future__ import annotations

import json
import logging
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from shopstream.events import EventValidationError, parse_event
from shopstream.ingestion.bronze import (
    BronzeWriter,
    RejectedMessage,
    ValidEvent,
    make_batch_id,
)
from shopstream.logging_setup import log_kv
from shopstream.streaming.base import EventConsumer

logger = logging.getLogger(__name__)


@dataclass
class IngestStats:
    received: int = 0
    valid: int = 0
    rejected: int = 0
    duplicates_dropped: int = 0
    batches: int = 0

    @property
    def dead_letter_ratio(self) -> float:
        return self.rejected / self.received if self.received else 0.0


class RecentIdFilter:
    """Bounded LRU of recently seen event ids: cheap first line of defence against re-sends."""

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def seen_before(self, event_id: str) -> bool:
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return True
        self._seen[event_id] = None
        if len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
        return False


def run_ingestion(
    consumer: EventConsumer,
    writer: BronzeWriter,
    *,
    batch_max_messages: int = 5_000,
    dedup_window: int = 200_000,
    idle_timeout_s: float = 0.0,
    run_log: Path | None = None,
) -> IngestStats:
    """Drain the stream. With `idle_timeout_s` > 0, keep waiting for new data that long."""
    stats = IngestStats()
    dedup = RecentIdFilter(dedup_window)
    idle_since: float | None = None

    while True:
        messages = consumer.poll(batch_max_messages, timeout_s=1.0)
        if not messages:
            idle_since = idle_since or time.monotonic()
            if time.monotonic() - idle_since >= idle_timeout_s:
                break
            time.sleep(0.25)
            continue
        idle_since = None

        batch_id = make_batch_id(messages)
        valid: list[ValidEvent] = []
        rejected: list[RejectedMessage] = []
        for message in messages:
            try:
                envelope = parse_event(message.value)
            except EventValidationError as exc:
                rejected.append(RejectedMessage(message, str(exc)))
                continue
            if dedup.seen_before(envelope.event_id):
                stats.duplicates_dropped += 1
                continue
            payload_json = json.dumps(envelope.payload, separators=(",", ":"), sort_keys=True)
            valid.append(ValidEvent(envelope, payload_json, message))

        ingested_at = datetime.now(UTC)
        writer.write_events(valid, batch_id, ingested_at)
        writer.write_dead_letters(rejected, batch_id, ingested_at)
        consumer.commit()  # only now is it safe to forget these messages

        stats.received += len(messages)
        stats.valid += len(valid)
        stats.rejected += len(rejected)
        stats.batches += 1
        log_kv(
            logger,
            "batch ingested",
            batch_id=batch_id,
            received=len(messages),
            valid=len(valid),
            rejected=len(rejected),
        )

    if run_log is not None:
        run_log.parent.mkdir(parents=True, exist_ok=True)
        with run_log.open("a") as fh:
            fh.write(
                json.dumps({"finished_at": datetime.now(UTC).isoformat(), **asdict(stats)}) + "\n"
            )
    return stats
