from __future__ import annotations

from collections import Counter
from datetime import date
from itertools import pairwise

import pytest

from shopstream.events import EventType, EventValidationError, parse_event
from shopstream.generator import SimulationConfig, simulate

SMALL = SimulationConfig(days=3, customers=60, products=20, orders_per_day=40)


@pytest.fixture(scope="module")
def result():  # type: ignore[no-untyped-def]
    return simulate(SMALL)


def test_same_seed_gives_identical_stream(result) -> None:  # type: ignore[no-untyped-def]
    again = simulate(SMALL)
    assert [m.value for m in again.messages] == [m.value for m in result.messages]


def test_different_seed_gives_different_stream(result) -> None:  # type: ignore[no-untyped-def]
    other = simulate(SimulationConfig(**{**SMALL.__dict__, "seed": 7}))
    assert [m.value for m in other.messages] != [m.value for m in result.messages]


def test_clean_events_are_all_valid_and_unique(result) -> None:  # type: ignore[no-untyped-def]
    ids = [e.event_id for e in result.clean_events]
    assert len(ids) == len(set(ids))
    assert Counter(e.event_type for e in result.clean_events).keys() == set(EventType)


def test_delivered_stream_contains_exactly_the_injected_faults(result) -> None:  # type: ignore[no-untyped-def]
    invalid = 0
    seen: Counter[str] = Counter()
    for message in result.messages:
        try:
            seen[parse_event(message.value).event_id] += 1
        except EventValidationError:
            invalid += 1
    assert invalid == result.stats["malformed"]
    assert sum(n - 1 for n in seen.values()) == result.stats["duplicates"]
    assert set(seen) == {e.event_id for e in result.clean_events}


def test_late_events_really_arrive_out_of_order(result) -> None:  # type: ignore[no-untyped-def]
    times = [parse_event(m.value).event_time for m in result.messages if _is_valid(m.value)]
    inversions = sum(1 for a, b in pairwise(times) if b < a)
    assert inversions > 0


def test_nothing_happens_after_the_simulation_window(result) -> None:  # type: ignore[no-untyped-def]
    end = SMALL.start_date.toordinal() + SMALL.days
    assert all(e.event_time.date().toordinal() <= end for e in result.clean_events)


def test_every_order_references_a_known_customer_and_product(result) -> None:  # type: ignore[no-untyped-def]
    customers = {e.payload["customer_id"] for e in result.iter_clean(EventType.CUSTOMER_UPDATED)}
    products = {e.payload["product_id"] for e in result.iter_clean(EventType.PRODUCT_UPSERTED)}
    for order in result.iter_clean(EventType.ORDER_PLACED):
        assert order.payload["customer_id"] in customers
        assert {i["product_id"] for i in order.payload["items"]} <= products  # type: ignore[attr-defined,union-attr]


def test_start_date_is_respected() -> None:
    cfg = SimulationConfig(**{**SMALL.__dict__, "start_date": date(2024, 6, 1), "days": 1})
    orders = list(simulate(cfg).iter_clean(EventType.ORDER_PLACED))
    assert orders
    assert all(o.event_time.date() == date(2024, 6, 1) for o in orders)


def _is_valid(raw: bytes) -> bool:
    try:
        parse_event(raw)
    except EventValidationError:
        return False
    return True
