from __future__ import annotations

from typing import Any

from shopstream.config import Settings
from shopstream.streaming.base import EventConsumer, EventPublisher
from shopstream.streaming.file_transport import FileConsumer, FilePublisher


def build_publisher(settings: Settings) -> EventPublisher:
    if settings.transport == "kafka":
        from shopstream.streaming.kafka_transport import KafkaPublisher

        return KafkaPublisher(settings.kafka_bootstrap_servers, settings.kafka_topic)
    return FilePublisher(settings.landing_dir)


def build_consumer(settings: Settings) -> EventConsumer:
    if settings.transport == "kafka":
        from shopstream.streaming.kafka_transport import KafkaConsumer

        return KafkaConsumer(
            settings.kafka_bootstrap_servers, settings.kafka_topic, settings.kafka_consumer_group
        )
    return FileConsumer(settings.landing_dir, settings.checkpoint_path)


def close_quietly(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        close()
