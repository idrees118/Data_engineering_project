"""The Kafka adapters are exercised against in-memory fakes of the confluent-kafka client."""

from __future__ import annotations

from typing import Any

import pytest

from shopstream.streaming.kafka_transport import KafkaConsumer, KafkaPublisher


class FakeProducer:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.full_once = False
        self.remaining = 0

    def produce(self, topic: str, key: bytes, value: bytes, on_delivery: Any) -> None:
        if self.full_once:
            self.full_once = False
            raise BufferError("queue full")
        self.sent.append((topic, key, value))

    def poll(self, timeout: float) -> None: ...

    def flush(self, timeout: float) -> int:
        return self.remaining


class FakeRecord:
    def __init__(
        self, partition: int, offset: int, key: bytes | None, value: bytes, error: Any = None
    ) -> None:
        self._p, self._o, self._k, self._v, self._e = partition, offset, key, value, error

    def error(self) -> Any:
        return self._e

    def key(self) -> bytes | None:
        return self._k

    def value(self) -> bytes:
        return self._v

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class FakeConsumer:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.records: list[FakeRecord] = []
        self.commits = 0
        self.subscribed: list[str] = []

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def consume(self, num_messages: int, timeout: float) -> list[FakeRecord]:
        return self.records[:num_messages]

    def commit(self, asynchronous: bool) -> None:
        assert asynchronous is False, "offsets must be committed synchronously"
        self.commits += 1

    def close(self) -> None: ...


def test_producer_is_configured_for_durability() -> None:
    pub = KafkaPublisher("b:9092", "t", producer_factory=FakeProducer)
    cfg = pub._producer.config
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"


def test_publisher_retries_when_local_queue_is_full() -> None:
    pub = KafkaPublisher("b:9092", "t", producer_factory=FakeProducer)
    pub._producer.full_once = True
    pub.publish("order-1", b"payload")
    assert pub._producer.sent == [("t", b"order-1", b"payload")]


def test_flush_fails_loudly_when_messages_are_undelivered() -> None:
    pub = KafkaPublisher("b:9092", "t", producer_factory=FakeProducer)
    pub._producer.remaining = 3
    with pytest.raises(RuntimeError, match="3 messages"):
        pub.flush()


def test_consumer_never_auto_commits_and_maps_records() -> None:
    consumer = KafkaConsumer("b:9092", "t", "g", consumer_factory=FakeConsumer)
    raw = consumer._consumer
    assert raw.config["enable.auto.commit"] is False
    assert raw.subscribed == ["t"]
    raw.records = [FakeRecord(0, 41, b"k", b"v"), FakeRecord(1, 7, None, b"w")]
    messages = consumer.poll(10)
    assert [(m.key, m.value, m.position) for m in messages] == [
        ("k", b"v", "0:41"),
        (None, b"w", "1:7"),
    ]
    consumer.commit()
    assert raw.commits == 1


def test_consumer_surfaces_broker_errors() -> None:
    consumer = KafkaConsumer("b:9092", "t", "g", consumer_factory=FakeConsumer)
    consumer._consumer.records = [FakeRecord(0, 0, None, b"", error="broker down")]
    with pytest.raises(RuntimeError, match="broker down"):
        consumer.poll(1)
