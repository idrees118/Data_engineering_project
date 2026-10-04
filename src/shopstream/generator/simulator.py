"""Deterministic e-commerce traffic simulator.

Produces the same business events a real shop emits (customers, catalogue changes, browsing
sessions, orders, payments, fulfilment) and then degrades the *delivery* of those events the way
real pipelines get degraded:

* duplicates   - at-least-once producers re-send the same event
* late arrival - mobile clients / retries deliver events hours after they happened
* malformed    - bad deploys emit payloads that violate the contract

The clean event set is returned next to the delivered messages so tests can reconcile the
pipeline's output against ground truth.
"""

from __future__ import annotations

import json
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from itertools import accumulate

from shopstream.events import Envelope, EventType, serialize_event

CATEGORIES = {
    "electronics": (25.0, 900.0),
    "home": (8.0, 250.0),
    "fashion": (12.0, 180.0),
    "sports": (10.0, 300.0),
    "books": (5.0, 45.0),
    "beauty": (4.0, 90.0),
}
COUNTRIES = ["DE", "FR", "NL", "ES", "IT", "PL", "SE", "PT"]
# Relative traffic by hour of day: quiet at night, peak in the evening.
HOUR_WEIGHTS = [1, 1, 1, 1, 1, 2, 3, 5, 6, 6, 6, 7, 8, 7, 6, 6, 7, 8, 10, 12, 12, 9, 5, 2]
WEEKDAY_FACTOR = [0.95, 0.9, 0.9, 0.95, 1.05, 1.25, 1.2]  # Monday..Sunday
NEXT_TIER = {"standard": "plus", "plus": "vip"}


@dataclass(frozen=True)
class SimulationConfig:
    seed: int = 42
    start_date: date = date(2025, 1, 1)
    days: int = 14
    customers: int = 500
    products: int = 80
    orders_per_day: int = 150
    duplicate_rate: float = 0.01
    late_rate: float = 0.02
    malformed_rate: float = 0.005
    max_lateness_hours: float = 36.0
    # Orders placed from this day on use order_placed schema v2 (None = never). Default: halfway.
    v2_from_day: int | None = -1


@dataclass(frozen=True)
class DeliveredMessage:
    key: str
    value: bytes


@dataclass
class SimulationResult:
    """`messages` is what arrives on the stream; `clean_events` is the ground truth."""

    messages: list[DeliveredMessage]
    clean_events: list[Envelope]
    stats: dict[str, int] = field(default_factory=dict)

    def iter_clean(self, event_type: EventType) -> Iterator[Envelope]:
        return (e for e in self.clean_events if e.event_type == event_type)


class _Simulator:
    def __init__(self, cfg: SimulationConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.events: list[Envelope] = []
        self.order_seq = 0
        self.payment_seq = 0
        self.session_seq = 0
        self.window_start = datetime.combine(cfg.start_date, time.min, tzinfo=UTC)
        self.window_end = self.window_start + timedelta(days=cfg.days)

        self.customer_ids = [f"cus_{i:05d}" for i in range(cfg.customers)]
        # Heavy-tailed customer activity: a few customers order a lot.
        weights = [1 / (rank + 1) ** 0.7 for rank in range(cfg.customers)]
        self.customer_cum_weights = list(accumulate(weights))
        self.customer_state: dict[str, dict[str, str]] = {}
        self.product_price: dict[str, float] = {}
        self.product_ids = [f"prd_{i:04d}" for i in range(cfg.products)]

    # -- helpers -----------------------------------------------------------------------------
    def _uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _emit(
        self,
        event_type: EventType,
        at: datetime,
        payload: dict[str, object],
        schema_version: int = 1,
    ) -> None:
        if at >= self.window_end:
            return  # has not happened yet at "now"
        self.events.append(
            Envelope(
                event_id=self._uuid(),
                event_type=event_type,
                schema_version=schema_version,
                event_time=at.replace(microsecond=0),
                payload=payload,
            )
        )

    def _uses_v2(self, day: date) -> bool:
        cutover = self.cfg.v2_from_day
        if cutover is None:
            return False
        if cutover < 0:
            cutover = self.cfg.days // 2
        return (day - self.cfg.start_date).days >= cutover

    def _random_moment(self, day: date) -> datetime:
        hour = self.rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        return datetime.combine(day, time(hour), tzinfo=UTC) + timedelta(
            minutes=self.rng.randrange(60), seconds=self.rng.randrange(60)
        )

    # -- reference data ----------------------------------------------------------------------
    def _seed_reference_data(self) -> None:
        onboarding = self.window_start - timedelta(days=1)
        for cid in self.customer_ids:
            state = {
                "email": f"{cid}@example.com",
                "country": self.rng.choice(COUNTRIES),
                "tier": "standard",
            }
            self.customer_state[cid] = state
            self._emit_customer(cid, onboarding + timedelta(seconds=self.rng.randrange(3600)))
        categories = list(CATEGORIES)
        for pid in self.product_ids:
            category = self.rng.choice(categories)
            low, high = CATEGORIES[category]
            price = round(self.rng.uniform(low, high), 2)
            self.product_price[pid] = price
            self._emit_product(pid, category, onboarding)
        self.product_category: dict[str, str] = {}
        for e in self.events:
            if e.event_type == EventType.PRODUCT_UPSERTED:
                self.product_category[str(e.payload["product_id"])] = str(e.payload["category"])

    def _emit_customer(self, customer_id: str, at: datetime) -> None:
        self._emit(
            EventType.CUSTOMER_UPDATED,
            at,
            {"customer_id": customer_id, **self.customer_state[customer_id]},
        )

    def _emit_product(self, product_id: str, category: str, at: datetime) -> None:
        self._emit(
            EventType.PRODUCT_UPSERTED,
            at,
            {
                "product_id": product_id,
                "name": f"{category.title()} item {product_id[-4:]}",
                "category": category,
                "unit_price": self.product_price[product_id],
                "currency": "EUR",
            },
        )

    # -- daily dynamics ----------------------------------------------------------------------
    def _simulate_day(self, day: date) -> None:
        rng = self.rng
        for cid in self.customer_ids:
            if rng.random() < 0.004:
                state = self.customer_state[cid]
                if state["tier"] in NEXT_TIER and rng.random() < 0.6:
                    state["tier"] = NEXT_TIER[state["tier"]]
                else:
                    state["country"] = rng.choice(COUNTRIES)
                self._emit_customer(cid, self._random_moment(day))
        for pid in self.product_ids:
            if rng.random() < 0.01:
                self.product_price[pid] = round(
                    self.product_price[pid] * rng.uniform(0.85, 1.15), 2
                )
                self._emit_product(pid, self.product_category[pid], self._random_moment(day))

        factor = WEEKDAY_FACTOR[day.weekday()] * rng.uniform(0.85, 1.15)
        for _ in range(int(self.cfg.orders_per_day * factor)):
            self._simulate_order(day)
        for _ in range(self.cfg.orders_per_day * 6):
            self._browsing_session(day, converting_customer=None)

    def _browsing_session(
        self,
        day: date,
        converting_customer: str | None,
        product_ids: list[str] | None = None,
        start: datetime | None = None,
    ) -> datetime:
        """Emit a session funnel. Converting sessions go all the way to checkout."""
        rng = self.rng
        self.session_seq += 1
        session_id = f"ses_{self.session_seq:08d}"
        customer = converting_customer
        if customer is None and rng.random() < 0.3:
            customer = rng.choices(self.customer_ids, cum_weights=self.customer_cum_weights)[0]
        steps = ["home", "category", "product", "cart", "checkout"]
        stop_after = (
            len(steps)
            if converting_customer
            else rng.choices(range(1, len(steps) + 1), weights=[30, 28, 25, 10, 7])[0]
        )
        at = start or self._random_moment(day)
        last = at
        product_pool = product_ids or [rng.choice(self.product_ids)]
        for step in steps[:stop_after]:
            product = rng.choice(product_pool) if step in {"product", "cart"} else None
            self._emit(
                EventType.PAGE_VIEWED,
                at,
                {
                    "session_id": session_id,
                    "customer_id": customer,
                    "product_id": product,
                    "page_type": step,
                },
            )
            last = at
            at += timedelta(seconds=rng.randrange(8, 120))
        return last

    def _simulate_order(self, day: date) -> None:
        rng = self.rng
        customer = rng.choices(self.customer_ids, cum_weights=self.customer_cum_weights)[0]
        chosen = rng.sample(self.product_ids, rng.choices([1, 2, 3, 4], weights=[50, 28, 15, 7])[0])
        items = [
            {
                "product_id": pid,
                "quantity": rng.choices([1, 2, 3], weights=[75, 18, 7])[0],
                "unit_price": self.product_price[pid],
            }
            for pid in chosen
        ]
        gross = round(sum(i["quantity"] * i["unit_price"] for i in items), 2)  # type: ignore[operator]
        discount = round(gross * rng.uniform(0.05, 0.15), 2) if rng.random() < 0.2 else 0.0

        session_start = self._random_moment(day)
        checkout_at = self._browsing_session(day, customer, chosen, session_start)
        placed_at = checkout_at + timedelta(seconds=rng.randrange(20, 240))

        self.order_seq += 1
        order_id = f"ord_{self.order_seq:07d}"
        order_payload: dict[str, object] = {
            "order_id": order_id,
            "customer_id": customer,
            "currency": "EUR",
            "items": items,
            "discount_amount": discount,
        }
        schema_version = 1
        if self._uses_v2(day):
            schema_version = 2
            order_payload["channel"] = rng.choices(
                ["web", "mobile", "marketplace"], weights=[55, 35, 10]
            )[0]
            if discount > 0:
                order_payload["coupon_code"] = f"SAVE{rng.choice([5, 10, 15])}"
        self._emit(EventType.ORDER_PLACED, placed_at, order_payload, schema_version)

        amount = round(gross - discount, 2)
        paid_at = self._payment_flow(order_id, amount, placed_at)
        if paid_at is None:
            self._status(order_id, "cancelled", placed_at + timedelta(hours=1))
            return
        if rng.random() < 0.03:  # customer cancels before dispatch; money goes back
            cancelled_at = paid_at + timedelta(hours=rng.uniform(1, 12))
            self._status(order_id, "cancelled", cancelled_at)
            self._status(order_id, "refunded", cancelled_at + timedelta(days=1))
            return
        shipped_at = paid_at + timedelta(hours=rng.uniform(6, 48))
        delivered_at = shipped_at + timedelta(days=rng.uniform(1, 5))
        self._status(order_id, "shipped", shipped_at)
        self._status(order_id, "delivered", delivered_at)
        if rng.random() < 0.04:
            self._status(order_id, "refunded", delivered_at + timedelta(days=rng.uniform(2, 10)))

    def _payment_flow(self, order_id: str, amount: float, placed_at: datetime) -> datetime | None:
        """Returns the time of the successful payment, or None if the customer gave up."""
        rng = self.rng
        method = rng.choices(["card", "paypal", "bank_transfer"], weights=[65, 25, 10])[0]
        at = placed_at + timedelta(seconds=rng.randrange(5, 90))
        succeeded = rng.random() < 0.93
        self._payment(order_id, amount, method, "succeeded" if succeeded else "failed", at)
        if succeeded:
            return at
        if rng.random() < 0.6:
            retry_at = at + timedelta(minutes=rng.uniform(2, 30))
            self._payment(order_id, amount, method, "succeeded", retry_at)
            return retry_at
        return None

    def _payment(
        self, order_id: str, amount: float, method: str, status: str, at: datetime
    ) -> None:
        self.payment_seq += 1
        self._emit(
            EventType.PAYMENT_PROCESSED,
            at,
            {
                "payment_id": f"pay_{self.payment_seq:08d}",
                "order_id": order_id,
                "amount": amount,
                "method": method,
                "status": status,
            },
        )

    def _status(self, order_id: str, status: str, at: datetime) -> None:
        self._emit(EventType.ORDER_STATUS_CHANGED, at, {"order_id": order_id, "status": status})

    # -- delivery degradation ----------------------------------------------------------------
    def _deliver(self) -> tuple[list[DeliveredMessage], dict[str, int]]:
        rng, cfg = self.rng, self.cfg
        arrivals: list[tuple[datetime, int, DeliveredMessage]] = []
        counters = {"late": 0, "duplicates": 0, "malformed": 0}

        def add(at: datetime, key: str, value: bytes) -> None:
            arrivals.append((at, len(arrivals), DeliveredMessage(key, value)))

        for event in self.events:
            key = str(
                event.payload.get("order_id")
                or event.payload.get("customer_id")
                or event.payload.get("product_id")
                or event.payload.get("session_id")
            )
            raw = serialize_event(event)
            arrival = event.event_time + timedelta(seconds=rng.uniform(0.2, 3))
            if rng.random() < cfg.late_rate:
                arrival += timedelta(hours=rng.uniform(2, cfg.max_lateness_hours))
                counters["late"] += 1
            add(arrival, key, raw)
            if rng.random() < cfg.duplicate_rate:
                add(arrival + timedelta(seconds=rng.uniform(1, 600)), key, raw)
                counters["duplicates"] += 1
            if rng.random() < cfg.malformed_rate:
                add(arrival, key, self._corrupt(event))
                counters["malformed"] += 1

        arrivals.sort(key=lambda a: (a[0], a[1]))
        return [m for _, _, m in arrivals], counters

    def _corrupt(self, event: Envelope) -> bytes:
        """Return a message that violates the contract in one of the ways seen in production."""
        doc = json.loads(serialize_event(event))
        doc["event_id"] = self._uuid()
        mode = self.rng.choice(["truncated", "missing_field", "bad_value", "unknown_type", "no_tz"])
        if mode == "truncated":
            return json.dumps(doc).encode()[: self.rng.randrange(10, 60)]
        if mode == "missing_field":
            doc["payload"].pop(next(iter(doc["payload"])))
        elif mode == "bad_value":
            payload = doc["payload"]
            if "items" in payload:
                payload["items"][0]["quantity"] = -1
            elif "amount" in payload:
                payload["amount"] = -5
            elif "unit_price" in payload:
                payload["unit_price"] = 0
            elif "status" in payload:
                payload["status"] = "teleported"
            else:
                payload["page_type"] = "???"
        elif mode == "unknown_type":
            doc["event_type"] = "order_teleported"
        else:
            doc["event_time"] = doc["event_time"].replace("Z", "").split("+")[0]
        return json.dumps(doc).encode()


def simulate(config: SimulationConfig | None = None) -> SimulationResult:
    cfg = config or SimulationConfig()
    sim = _Simulator(cfg)
    sim._seed_reference_data()
    for offset in range(cfg.days):
        sim._simulate_day(cfg.start_date + timedelta(days=offset))
    messages, counters = sim._deliver()
    sim.events.sort(key=lambda e: e.event_time)
    stats = {
        "clean_events": len(sim.events),
        "delivered_messages": len(messages),
        "orders": sim.order_seq,
        **counters,
    }
    return SimulationResult(messages=messages, clean_events=sim.events, stats=stats)
