"""Tests against real services in containers: a Redpanda (Kafka API) broker and an S3 server.

These prove what fakes cannot: that the Kafka adapter really does at-least-once delivery with
manual commits against a broker, and that the S3 lake works on a real S3-compatible server.
They need a Docker daemon and skip when there is none; CI runs them in a dedicated job
(`pytest -m docker`).
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path

import boto3
import duckdb
import pytest

pytest.importorskip("testcontainers", reason="testcontainers is not installed")
pytest.importorskip("confluent_kafka", reason="confluent-kafka is not installed")

from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

try:
    from testcontainers.community.kafka import RedpandaContainer
except ImportError:  # older testcontainers
    from testcontainers.kafka import RedpandaContainer  # type: ignore[no-redef]

from shopstream.events import EventType
from shopstream.generator import SimulationConfig, simulate
from shopstream.ingestion import BronzeWriter, run_ingestion
from shopstream.quality import run_bronze_checks
from shopstream.storage import Lake, S3Options
from shopstream.streaming.base import Message
from shopstream.streaming.kafka_transport import KafkaConsumer, KafkaPublisher
from tests.integration.s3_support import dbt_build_on_s3, ingest_simulation

REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v24.2.7"
# MinIO no longer publishes community images (Docker Hub repo removed, quay.io closed), so the
# S3 side uses Adobe's S3Mock, a maintained S3-compatible server. docker-compose.s3.yml uses the
# same image, so what CI verifies is what the compose stack runs.
S3_IMAGE = "adobe/s3mock:latest"
S3_PORT = 9090


def _docker_available() -> bool:
    try:
        import docker

        docker.from_env().ping()
    except Exception:
        return False
    return True


pytestmark = [
    pytest.mark.integration,
    pytest.mark.docker,
    pytest.mark.skipif(not _docker_available(), reason="no Docker daemon available"),
]


# -- Redpanda / Kafka -----------------------------------------------------------------------------


@pytest.fixture(scope="module")
def broker() -> Iterator[str]:
    with RedpandaContainer(image=REDPANDA_IMAGE) as container:
        yield container.get_bootstrap_server()


def _create_topic(broker: str, partitions: int = 3) -> str:
    name = f"shopstream.test.{uuid.uuid4().hex[:8]}"
    # keep a reference: if the admin client is garbage collected, its pending futures fail
    admin = AdminClient({"bootstrap.servers": broker})
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    futures[name].result(timeout=30)
    return name


def _drain(consumer: KafkaConsumer, want: int | None = None, idle_polls: int = 8) -> list[Message]:
    """Poll until `want` messages arrived, or (if None) until the topic has been quiet for a while.

    The first polls of a new consumer group are empty while it joins, so quiet is judged over
    several polls, not one.
    """
    got: list[Message] = []
    empty = 0
    while (want is None and empty < idle_polls) or (want is not None and len(got) < want):
        remaining = 500 if want is None else want - len(got)
        batch = list(consumer.poll(remaining, timeout_s=1.0))
        empty = 0 if batch else empty + 1
        got += batch
        if want is not None and empty > 60:
            raise AssertionError(f"timed out after {len(got)} of {want} messages")
    return got


def test_full_pipeline_through_a_real_broker(broker: str, tmp_path: Path) -> None:
    result = simulate(SimulationConfig(days=2, customers=40, products=15, orders_per_day=25))
    topic = _create_topic(broker)

    publisher = KafkaPublisher(broker, topic)
    for message in result.messages:
        publisher.publish(message.key, message.value)
    publisher.flush()

    consumer = KafkaConsumer(broker, topic, group_id=f"g-{uuid.uuid4().hex[:6]}")
    try:
        stats = run_ingestion(
            consumer,
            BronzeWriter(tmp_path / "bronze"),
            batch_max_messages=500,
            idle_timeout_s=10,
        )
    finally:
        consumer.close()

    assert stats.received == len(result.messages)
    assert stats.valid == len(result.clean_events)
    assert stats.rejected == result.stats["malformed"]
    assert stats.duplicates_dropped == result.stats["duplicates"]
    rows = duckdb.sql(
        f"select count(*) from read_parquet('{tmp_path}/bronze/events/**/*.parquet')"
    ).fetchone()[0]  # type: ignore[index]
    assert rows == len(result.clean_events)


def test_messages_with_the_same_key_keep_their_order(broker: str) -> None:
    topic = _create_topic(broker, partitions=3)
    publisher = KafkaPublisher(broker, topic)
    for i in range(60):
        publisher.publish("order-42", str(i).encode())
        publisher.publish(f"noise-{i}", b"x")  # other keys land on other partitions
    publisher.flush()

    consumer = KafkaConsumer(broker, topic, group_id=f"g-{uuid.uuid4().hex[:6]}")
    try:
        messages = _drain(consumer, want=120)
    finally:
        consumer.close()
    ordered = [int(m.value) for m in messages if m.key == "order-42"]
    assert ordered == list(range(60))


def test_uncommitted_messages_are_redelivered_and_committed_ones_are_not(broker: str) -> None:
    topic = _create_topic(broker)
    publisher = KafkaPublisher(broker, topic)
    for i in range(300):
        publisher.publish(f"k{i}", str(i).encode())
    publisher.flush()
    group = f"g-{uuid.uuid4().hex[:6]}"

    first = KafkaConsumer(broker, topic, group_id=group)
    committed = {m.value for m in _drain(first, want=100)}
    first.commit()  # these 100 are durably processed
    in_flight = {m.value for m in _drain(first, want=100)}
    first.close()  # crash: the next 100 were read but never committed

    second = KafkaConsumer(broker, topic, group_id=group)
    try:
        redelivered = {m.value for m in _drain(second)}
    finally:
        second.close()

    assert redelivered.isdisjoint(committed), "committed messages must not come back"
    assert in_flight <= redelivered, "nothing read but uncommitted may be lost"
    assert len(committed) + len(redelivered) == 300


# -- S3 server -----------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def s3_server() -> Iterator[tuple[str, str, str]]:
    """An S3-compatible server. (Name kept short: it yields (endpoint_url, key, secret).)"""
    container = DockerContainer(S3_IMAGE).with_exposed_ports(S3_PORT)
    with container:
        wait_for_logs(container, "Started S3MockApplication", timeout=90)
        endpoint_url = (
            f"http://{container.get_container_host_ip()}:{container.get_exposed_port(S3_PORT)}"
        )
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name="us-east-1",
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        client.create_bucket(Bucket="shopstream-lake")
        yield endpoint_url, "test", "test"


def _s3_lake(server: tuple[str, str, str], name: str) -> Lake:
    endpoint_url, key, secret = server
    return Lake(f"s3://shopstream-lake/{name}", S3Options(endpoint_url, key, secret))


def test_ingestion_and_quality_gates_on_a_real_s3_server(s3_server: tuple[str, str, str]) -> None:
    lake = _s3_lake(s3_server, f"quality-{uuid.uuid4().hex[:6]}")
    valid, _ = ingest_simulation(lake)
    assert valid > 0
    assert lake.has_files("events") and lake.has_files("dead_letter")
    results = run_bronze_checks(lake, max_dead_letter_ratio=0.05, max_freshness_hours=None)
    assert all(r.passed for r in results), results


def test_dbt_builds_from_a_real_s3_server(s3_server: tuple[str, str, str], tmp_path: Path) -> None:
    lake = _s3_lake(s3_server, f"dbt-{uuid.uuid4().hex[:6]}")
    valid, _ = ingest_simulation(lake)
    dbt_build_on_s3(lake, s3_server[0], s3_server[1], s3_server[2], tmp_path)
    con = duckdb.connect(str(tmp_path / "warehouse" / "shopstream.duckdb"), read_only=True)
    try:
        assert con.sql("select count(*) from staging.stg_events").fetchone()[0] == valid  # type: ignore[index]
        assert con.sql("select count(*) from marts.fct_orders").fetchone()[0] > 0  # type: ignore[index]
    finally:
        con.close()


def test_event_types_are_all_present_in_the_simulation() -> None:
    # guards the Kafka test above against silently running on a degenerate dataset
    result = simulate(SimulationConfig(days=2, customers=40, products=15, orders_per_day=25))
    assert {e.event_type for e in result.clean_events} == set(EventType)
