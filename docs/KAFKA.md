# Kafka Decoupling Design

This project uses a broker abstraction for the write path. The current implementation ships a deterministic in-memory backend for local development and tests; the contract is shaped so a Kafka-backed adapter can be added without changing gateway, distributor, or ingester business logic.

## Goals

- Decouple external ingestion from WAL and chunk ownership.
- Preserve replayable normalized events before storage-side processing.
- Provide explicit consumer groups, offsets, lag, retry, and dead-letter behavior.
- Keep local and CI environments runnable without requiring Kafka.

## Topic Model

### `logs.normalized.v1`

Primary topic for canonical `model.Event` records after compatibility translation and tenant defaulting.

Partition key:

- `event.StreamKey()`

Partitioning by stream keeps a tenant stream ordered while allowing unrelated streams to spread across partitions.

### `logs.normalized.retry.v1`

Retry topic for records that fail processing but have not exhausted the attempt budget.

Required metadata:

- original event
- attempt count
- last error
- original key

### `logs.normalized.dlq.v1`

Dead-letter topic for poison records that exceed the configured attempt budget.

DLQ records must be inspectable and replayable after an operator or automated repair job fixes the source issue.

## Consumer Groups

### `gateway-local-writers`

Used by the current local gateway runtime. It consumes normalized and retry topics into the single-node engine so local runs remain queryable without a separate ingester service.

Future groups:

- `ingester-writers`: distributed WAL/chunk consumers
- `index-builders`: asynchronous segment builders
- `dlq-inspectors`: operator tooling and replay jobs

## Offset Semantics

Each consumer group tracks committed offsets per topic and partition.

Processing rule:

1. Poll from the group offset.
2. Process the message.
3. Publish retry or DLQ if processing fails.
4. Commit the original message after successful processing or after handoff to retry/DLQ.

This gives at-least-once behavior. Downstream consumers must stay idempotent by stream key, timestamp, and payload identity.

## Lag Metrics

The broker reports:

- topic high watermark per partition
- committed offsets per group
- lag per `group:topic`
- total messages per topic

The gateway exposes these through:

- `GET /api/admin/queue`

## Local Backend

The local backend is implemented in `internal/engine/queue`.

It supports:

- deterministic partitioning
- publish, poll, and commit
- retry and dead-letter handoff
- lag accounting
- tests without external infrastructure

It does not provide:

- durable storage across process restarts
- multi-process coordination
- Kafka protocol compatibility

## Kafka Adapter Boundary

A production Kafka adapter should implement the same `queue.Broker` interface.

Expected Kafka mapping:

- `Publish` maps to producer send with topic and key.
- `Poll` maps to consumer group fetch.
- `Commit` maps to offset commit after processing.
- `Stats` maps to committed offsets and partition high-watermarks from Kafka admin APIs.

Recommended client:

- `segmentio/kafka-go` for a minimal Go-native adapter, or
- `confluent-kafka-go` where librdkafka operational parity is required.

## Failure Policy

- Transient processor errors go to `logs.normalized.retry.v1`.
- Poison records go to `logs.normalized.dlq.v1`.
- The original message is committed only after processing succeeds or after retry/DLQ handoff succeeds.
- Queue publish failures surface as ingest errors in local mode.

## Current Implementation Status

- Topic constants are implemented.
- Memory broker is implemented.
- Kafka broker adapter is implemented with `segmentio/kafka-go`.
- Consumer group commit and lag accounting are implemented.
- Retry and DLQ handoff are implemented.
- Gateway producer path is implemented.
- Local writer consumer is implemented.
