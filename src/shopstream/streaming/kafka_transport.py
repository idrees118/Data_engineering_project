"""Kafka / Redpanda transport built on confluent-kafka (install with the `kafka` extra)."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from typing import Any

from shopstream.streaming.base import Message

logger = logging.getLogger(__name__)


def _import_kafka() -> Any:
    try:
        import confluent_kafka
    except ImportError as exc:  # pragma: no cover - exercised only without the extra
        raise RuntimeError("Kafka transport needs `pip install shopstream[kafka]`") from exc
    return confluent_kafka


class KafkaPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        producer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        factory = producer_factory or _import_kafka().Producer
        self._topic = topic
        self._producer = factory(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,  # no duplicates from producer retries
                "acks": "all",
                "compression.type": "zstd",
                "linger.ms": 50,
            }
        )

    def publish(self, key: str, value: bytes) -> None:
        while True:
            try:
                self._producer.produce(
                    self._topic, key=key.encode(), value=value, on_delivery=self._on_delivery
                )
                break
            except BufferError:  # local queue full: let delivery callbacks drain it
                self._producer.poll(0.5)
        self._producer.poll(0)

    @staticmethod
    def _on_delivery(err: Any, msg: Any) -> None:
        if err is not None:
            logger.error("delivery failed: %s", err)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} messages were not delivered")


class KafkaConsumer:
    """Manual-commit consumer: offsets advance only after the batch is safely in the lake."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        consumer_factory: Callable[[dict[str, Any]], Any] | None = None,
    ) -> None:
        factory = consumer_factory or _import_kafka().Consumer
        self._consumer = factory(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])

    def poll(self, max_messages: int, timeout_s: float = 1.0) -> Sequence[Message]:
        records = self._consumer.consume(num_messages=max_messages, timeout=timeout_s)
        out: list[Message] = []
        for record in records:
            if record.error():
                raise RuntimeError(f"kafka error: {record.error()}")
            key = record.key().decode() if record.key() else None
            out.append(Message(key, record.value(), f"{record.partition()}:{record.offset()}"))
        return out

    def commit(self) -> None:
        self._consumer.commit(asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
