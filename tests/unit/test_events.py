from __future__ import annotations

import json

import pytest

from shopstream.events import EventType, EventValidationError, parse_event, serialize_event
from tests.conftest import make_event

ORDER = {
    "order_id": "ord_1",
    "customer_id": "cus_1",
    "currency": "EUR",
    "discount_amount": 0,
    "items": [{"product_id": "prd_1", "quantity": 2, "unit_price": 9.99}],
}


def _raw(**changes: object) -> bytes:
    doc = json.loads(serialize_event(make_event()))
    doc.update(changes)
    return json.dumps(doc).encode()


def test_valid_event_round_trips(raw_event: bytes) -> None:
    envelope = parse_event(raw_event)
    assert envelope.event_type is EventType.ORDER_STATUS_CHANGED
    assert envelope.payload["status"] == "shipped"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (b"not json at all", "invalid_json"),
        (b'["a", "list"]', "invalid_json"),
        (b'{"truncated": ', "invalid_json"),
        (b"\xff\xfe", "invalid_json"),
    ],
)
def test_undecodable_messages_are_rejected(raw: bytes, expected: str) -> None:
    with pytest.raises(EventValidationError, match=expected):
        parse_event(raw)


def test_unknown_event_type_is_rejected() -> None:
    with pytest.raises(EventValidationError, match="event_type"):
        parse_event(_raw(event_type="order_teleported"))


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(EventValidationError, match="timezone-aware"):
        parse_event(_raw(event_time="2025-01-05T12:00:00"))


def test_unsupported_schema_version_is_rejected() -> None:
    with pytest.raises(EventValidationError, match="schema_version"):
        parse_event(_raw(schema_version=3))


def test_payload_with_unexpected_field_is_rejected() -> None:
    with pytest.raises(EventValidationError, match="schema_violation"):
        parse_event(_raw(payload={"order_id": "o", "status": "shipped", "surprise": 1}))


def test_payload_with_bad_enum_value_is_rejected() -> None:
    with pytest.raises(EventValidationError, match="status"):
        parse_event(_raw(payload={"order_id": "o", "status": "teleported"}))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda o: o["items"][0].update(quantity=0),
        lambda o: o["items"][0].update(unit_price=-1),
        lambda o: o.update(items=[]),
        lambda o: o.update(discount_amount=-1),
        lambda o: o.update(items=o["items"] * 2),  # same product twice
    ],
    ids=["zero_qty", "negative_price", "no_items", "negative_discount", "duplicate_product"],
)
def test_order_business_rules(mutation) -> None:  # type: ignore[no-untyped-def]
    order = json.loads(json.dumps(ORDER))
    mutation(order)
    event = make_event(EventType.ORDER_PLACED, order)
    with pytest.raises(EventValidationError):
        parse_event(serialize_event(event))


def test_valid_order_is_accepted() -> None:
    parse_event(serialize_event(make_event(EventType.ORDER_PLACED, ORDER)))


def _order_event(version: int, **extra: object) -> bytes:
    return serialize_event(
        make_event(EventType.ORDER_PLACED, {**ORDER, **extra}, schema_version=version)
    )


def test_v1_order_without_new_fields_still_parses() -> None:
    parse_event(_order_event(1))


def test_v2_order_accepts_the_new_optional_fields() -> None:
    envelope = parse_event(_order_event(2, channel="mobile", coupon_code="SAVE10"))
    assert envelope.schema_version == 2


def test_v2_order_without_optional_fields_parses() -> None:
    parse_event(_order_event(2))


def test_v1_rejects_fields_that_only_exist_in_v2() -> None:
    with pytest.raises(EventValidationError, match="channel"):
        parse_event(_order_event(1, channel="mobile"))


def test_v2_rejects_unknown_channel() -> None:
    with pytest.raises(EventValidationError, match="channel"):
        parse_event(_order_event(2, channel="carrier_pigeon"))


def test_event_types_without_a_v2_reject_version_2() -> None:
    with pytest.raises(EventValidationError, match="no version 2"):
        parse_event(_raw(schema_version=2))
