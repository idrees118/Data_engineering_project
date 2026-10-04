from __future__ import annotations

from datetime import UTC, datetime

import pytest

from shopstream.events import Envelope, EventType, serialize_event


def make_event(
    event_type: EventType = EventType.ORDER_STATUS_CHANGED,
    payload: dict[str, object] | None = None,
    event_id: str = "11111111-2222-4333-8444-555555555555",
    **overrides: object,
) -> Envelope:
    return Envelope(
        event_id=event_id,
        event_type=event_type,
        event_time=datetime(2025, 1, 5, 12, 0, tzinfo=UTC),
        payload=payload if payload is not None else {"order_id": "ord_1", "status": "shipped"},
        **overrides,  # type: ignore[arg-type]
    )


@pytest.fixture
def raw_event() -> bytes:
    return serialize_event(make_event())
