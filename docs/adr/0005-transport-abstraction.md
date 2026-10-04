# 0005. Transport abstraction with a file-backed log

**Status:** accepted

## Context
Kafka is the realistic transport, but requiring Docker to run tests or a demo makes the project
harder to review and slower to iterate on.

## Decision
Ingestion depends on two small protocols (`EventPublisher`, `EventConsumer`). Kafka/Redpanda and a
file-backed append-only log both implement them with the same semantics: ordered, explicit commit,
replay from the last committed position.

## Consequences
* Ingestion logic is tested with no broker; the Kafka adapter is tested against a fake client that
  asserts the settings that matter (idempotent producer, `acks=all`, manual synchronous commits).
* The Kafka adapter has not been exercised against a live broker in CI. `docker-compose.yml`
  provides the stack for doing so; adding a Testcontainers-based test is on the roadmap.
