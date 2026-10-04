"""Event contracts.

Every message on the stream is an *envelope* wrapping a typed *payload*. The envelope is
stable across event types; payload schemas are versioned per type. Producers and the
ingestion job both validate against these models, so the contract lives in exactly one place.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

Money = Annotated[float, Field(ge=0, allow_inf_nan=False)]
PositiveMoney = Annotated[float, Field(gt=0, allow_inf_nan=False)]


class EventType(StrEnum):
    CUSTOMER_UPDATED = "customer_updated"
    PRODUCT_UPSERTED = "product_upserted"
    PAGE_VIEWED = "page_viewed"
    ORDER_PLACED = "order_placed"
    PAYMENT_PROCESSED = "payment_processed"
    ORDER_STATUS_CHANGED = "order_status_changed"


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CustomerUpdated(_Payload):
    customer_id: str = Field(min_length=1)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    country: str = Field(min_length=2, max_length=2)
    tier: Literal["standard", "plus", "vip"]


class ProductUpserted(_Payload):
    product_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    category: str = Field(min_length=1)
    unit_price: PositiveMoney
    currency: Literal["EUR"] = "EUR"


class PageViewed(_Payload):
    session_id: str = Field(min_length=1)
    customer_id: str | None = None
    product_id: str | None = None
    page_type: Literal["home", "category", "product", "cart", "checkout"]


class OrderItem(_Payload):
    product_id: str = Field(min_length=1)
    quantity: int = Field(ge=1, le=100)
    unit_price: PositiveMoney


class OrderPlaced(_Payload):
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    currency: Literal["EUR"] = "EUR"
    items: list[OrderItem] = Field(min_length=1)
    discount_amount: Money = 0.0

    @field_validator("items")
    @classmethod
    def _unique_products(cls, items: list[OrderItem]) -> list[OrderItem]:
        ids = [i.product_id for i in items]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate product_id in order items")
        return items


class OrderPlacedV2(OrderPlaced):
    """v2 adds two *optional* fields, so every v1 payload is also a valid v2 payload."""

    channel: Literal["web", "mobile", "marketplace"] | None = None
    coupon_code: str | None = Field(default=None, min_length=1)


class PaymentProcessed(_Payload):
    payment_id: str = Field(min_length=1)
    order_id: str = Field(min_length=1)
    amount: PositiveMoney
    method: Literal["card", "paypal", "bank_transfer"]
    status: Literal["succeeded", "failed"]


class OrderStatusChanged(_Payload):
    order_id: str = Field(min_length=1)
    status: Literal["shipped", "delivered", "cancelled", "refunded"]


PAYLOAD_MODELS: dict[EventType, type[_Payload]] = {
    EventType.CUSTOMER_UPDATED: CustomerUpdated,
    EventType.PRODUCT_UPSERTED: ProductUpserted,
    EventType.PAGE_VIEWED: PageViewed,
    EventType.ORDER_PLACED: OrderPlaced,
    EventType.PAYMENT_PROCESSED: PaymentProcessed,
    EventType.ORDER_STATUS_CHANGED: OrderStatusChanged,
}

# (event_type, schema_version) -> payload model. Adding a version is one line here plus a new
# model; the envelope never changes. Versions are additive: v2 must accept every v1 payload.
VERSIONED_PAYLOAD_MODELS: dict[tuple[EventType, int], type[_Payload]] = {
    **{(event_type, 1): model for event_type, model in PAYLOAD_MODELS.items()},
    (EventType.ORDER_PLACED, 2): OrderPlacedV2,
}
SUPPORTED_SCHEMA_VERSIONS = {version for _, version in VERSIONED_PAYLOAD_MODELS}


class Envelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=8)
    event_type: EventType
    schema_version: int = 1
    event_time: datetime
    payload: dict[str, object]

    @field_validator("event_time")
    @classmethod
    def _require_tz(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_time must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("schema_version")
    @classmethod
    def _known_version(cls, value: int) -> int:
        if value not in SUPPORTED_SCHEMA_VERSIONS:
            raise ValueError(f"unsupported schema_version {value}")
        return value


class EventValidationError(ValueError):
    """Raised when a raw message does not satisfy the event contract."""


def parse_event(raw: bytes) -> Envelope:
    """Decode and validate one raw message. Raises EventValidationError with a short reason."""
    try:
        data = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventValidationError(f"invalid_json: {exc}") from exc
    if not isinstance(data, dict):
        raise EventValidationError("invalid_json: top-level value is not an object")
    try:
        envelope = Envelope.model_validate(data)
        model = VERSIONED_PAYLOAD_MODELS.get((envelope.event_type, envelope.schema_version))
        if model is None:
            raise EventValidationError(
                f"schema_violation: schema_version: {envelope.event_type.value} "
                f"has no version {envelope.schema_version}"
            )
        model.model_validate(envelope.payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(p) for p in first["loc"])
        raise EventValidationError(f"schema_violation: {location}: {first['msg']}") from exc
    return envelope


def serialize_event(envelope: Envelope) -> bytes:
    return envelope.model_dump_json().encode("utf-8")
