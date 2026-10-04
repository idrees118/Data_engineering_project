"""Transport abstraction.

The ingestion job only knows about these two protocols. Kafka (Redpanda) is the production
transport; the file transport gives the same semantics (ordered log, explicit commit, replay
from the last committed position) so the pipeline can run and be tested without a broker.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Message:
    key: str | None
    value: bytes
    position: str  # opaque, transport specific: "partition:offset" or "segment:line"


class EventPublisher(Protocol):
    def publish(self, key: str, value: bytes) -> None: ...

    def flush(self) -> None: ...


class EventConsumer(Protocol):
    def poll(self, max_messages: int, timeout_s: float = 1.0) -> Sequence[Message]:
        """Return up to `max_messages`; an empty sequence means nothing new right now."""

    def commit(self) -> None:
        """Mark everything returned by `poll` so far as durably processed."""

    def close(self) -> None: ...
