# Implementation Checklist

This checklist is intended to drive future implementation passes without re-planning. Each item should be treated as a concrete deliverable with tests, docs, and explicit exit criteria.

## Status Legend

- `[x]` complete in repository
- `[~]` partially complete, scaffold exists but production behavior is missing
- `[ ]` not started

## Phase 0: Bootstrap

### Repository and Runtime

- [x] Create repository structure, Go module, and service entrypoints.
- [x] Add shared config loading and HTTP runtime bootstrapping.
- [x] Add reusable health, readiness, and service metadata endpoints.
- [x] Add build, test, and format entrypoints through `Makefile`.
- [x] Add container build scaffold through `Dockerfile`.

### Documentation

- [x] Write core product spec.
- [x] Write architecture guide.
- [x] Write API contract draft.
- [x] Write data model guide.
- [x] Write security, operations, and roadmap docs.
- [x] Record stack, indexing, and deployment ADRs.

### Deployment Scaffolding

- [x] Add Docker Compose dependency stack.
- [x] Add Helm chart scaffold for Kubernetes deployment.
- [x] Describe Kubernetes and hybrid bare-metal support in docs.

### Exit Criteria

- [x] Repository builds with `go build ./...`.
- [x] Repository tests pass with `go test ./...`.
- [x] Core documentation exists for future implementation passes.

## Phase 1: Single-Node Core

### 1. Domain Model and Validation

- [x] Define canonical event structure.
- [x] Add canonical event validation.
- [x] Add stable stream-key derivation.
- [x] Add query request validation.
- [x] Add size-budget validation for body, labels, and parsed fields.
- [x] Add explicit serialization contract tests for future OTLP and Loki adapters.

### 2. Tenant Policy and Local Control State

- [x] Implement in-memory tenant config store.
- [x] Add default tenant bootstrap policy.
- [x] Add tenant update validation for searchable fields and label policies.
- [x] Add local snapshot persistence for tenant configuration.
- [x] Add policy versioning and reload semantics.

### 3. Parser Pipeline Engine

- [x] Implement JSON stage.
- [x] Implement logfmt stage.
- [x] Implement regex stage with named captures.
- [x] Implement static enrichment stage.
- [x] Apply reserved-label and allow/deny promotion rules.
- [x] Add timestamp override extraction.
- [x] Add severity normalization stage.
- [x] Add drop/filter stage support.
- [x] Add field rename and redact stages.
- [x] Add parser error accounting and partial-failure policy.

### 4. Single-Node Durability Path

- [x] Implement append-only WAL writer.
- [x] Implement WAL replay reader.
- [x] Add local chunk builder.
- [x] Add zstd chunk encoding and decoding.
- [x] Add WAL segment rotation.
- [x] Add checksums for WAL and chunk payloads.
- [x] Add chunk metadata summaries: min/max timestamp, label set, searchable field set.
- [x] Add chunk flush triggers by event count, time window, and byte budget.
- [x] Add fsync policy abstraction for performance testing.

### 5. Local Indexing and Query

- [x] Implement in-memory label postings.
- [x] Implement in-memory structured-field postings.
- [x] Implement local query execution for label, field, and text filters.
- [x] Add a single-node engine that ties together tenant store, parser, WAL, and index.
- [x] Add time-partitioned segment abstraction.
- [x] Add cursor-based pagination.
- [x] Add query stats split by candidate streams, candidate chunks, and scanned events.
- [x] Add regex text query support with safety limits.
- [x] Add tail query semantics.

### 6. Service Wiring

- [x] Wire `gateway` ingest handlers to the single-node engine.
- [x] Add a native search endpoint backed by the single-node engine.
- [x] Add local admin endpoint to inspect tenants and parser pipelines.
- [x] Add local config/env flags for WAL path, chunk thresholds, and parser defaults.

### 7. Test Coverage

- [x] Unit test event validation.
- [x] Unit test parser stage behavior.
- [x] Unit test WAL append and replay.
- [x] Unit test chunk encode and decode.
- [x] Unit test local index query behavior.
- [x] Unit test single-node engine append, query, and replay.
- [x] Add HTTP integration tests for native ingest and query.
- [x] Add malformed-payload tests for parser and validation failures.
- [x] Add restart tests ensuring replayed data is queryable without duplication.

### Exit Criteria

- [x] OTLP ingest succeeds on one node end to end.
- [x] Native search endpoint returns logs by labels and structured fields.
- [x] Restart recovery preserves acknowledged writes without duplication.
- [x] Single-node read/write behavior is documented and demoable locally.

## Phase 2: Distributed Write Path

### 1. Coordination and Membership

- [x] Implement etcd-backed ring membership model.
- [x] Define shard ownership records and lease renewal.
- [x] Add instance health and readiness transitions into ring state.
- [x] Add rebalance semantics for join and leave.
- [x] Add minimal anti-entropy reconciliation for stale ownership metadata.

### 2. Distributor and Replication

- [x] Implement stream fingerprinting compatible with the canonical label model.
- [x] Route each stream to a primary plus replica ingesters.
- [x] Enforce per-tenant write limits before replication fan-out.
- [x] Define acknowledgment semantics for partial replica failure.
- [x] Add per-ingester backpressure signaling to the distributor.

### 3. Kafka Decoupling

- [x] Define Kafka-compatible topic model for normalized, retry, and dead-letter events.
- [x] Add producer path from gateway into the broker abstraction.
- [x] Add consumer groups for write-path workers using the local broker backend.
- [x] Add retry and dead-letter policy for malformed or poison messages.
- [x] Add offset lag metrics and replay controls through broker stats.
- [x] Add network Kafka adapter for production clusters.

### 4. Object Storage Flush

- [x] Persist chunk payloads to the object-store abstraction using the local filesystem backend.
- [x] Persist immutable segment manifests to the object-store abstraction.
- [x] Record local manifest and flush metadata for chunks and time partitions.
- [x] Add idempotency and checksum conflict behavior around object uploads.
- [x] Add retry policy around transient object-store failures.
- [x] Add PostgreSQL-backed manifest repository.
- [x] Add local cache invalidation and hydration rules.

### 5. Control-Plane Distribution

- [x] Add PostgreSQL-backed tenant repository.
- [x] Add control-plane APIs for policy updates using the persisted local snapshot.
- [x] Add snapshot refresh semantics for data-plane services via file-mtime reload.
- [x] Add schema compatibility checks for policy changes.
- [x] Add audit trail for control-plane writes.

### Exit Criteria

- [x] Multi-node ingestion balances across ingesters.
- [x] Replica placement is visible and debuggable.
- [x] WAL plus replication survive a single-node failure within documented guarantees.
- [x] Normalized events can be replayed safely from Kafka.

## Phase 3: Query and Search Acceleration

### 1. Query Planning

- [x] Implement query parsing into internal plan nodes.
- [x] Split queries by tenant, time partition, and shard.
- [x] Add planner cost heuristics based on label and field selectivity.
- [x] Add partial-result and timeout semantics into the planner contract.
- [x] Expose query explain output for debugging.

### 2. Segment and Chunk Pruning

- [x] Add chunk metadata pruning by time and label summaries.
- [x] Add bloom filters for body-text pruning.
- [x] Add token summaries or lightweight inverted text structures for hot data.
- [x] Add searchable-field dictionary metadata per segment.
- [x] Add planner-side intersection ordering to minimize scan cost.

### 3. Query Execution

- [x] Add querier RPC contract and execution worker pool.
- [x] Add ordered merge of shard responses.
- [x] Add cursor resume semantics across shards.
- [x] Add bounded regex evaluation with timeout and complexity guardrails.
- [x] Add tail execution path for hot streams.

### 4. Caching

- [x] Add query result cache layer with configurable backend and TTL.
- [x] Add metadata cache for chunk manifests and chunk bodies.
- [x] Add local chunk cache warming policy.
- [x] Add cache key versioning to prevent schema conflicts.
- [x] Add cache observability and invalidation semantics.

### Exit Criteria

- [x] Query planner reduces scan volume for common field-aware workloads.
- [x] Recent-data search works with bounded text predicates.
- [x] Query explain output is usable for operator debugging.
- [x] Cache hit ratios and planner statistics are observable.

## Phase 4: Enterprise Hardening

### 1. Security and Identity

- [x] Add OIDC auth for user and agent traffic.
- [x] Add service-to-service mTLS.
- [x] Add scope-based authz for ingest, query, and admin paths.
- [x] Add audit logs for admin mutations and privileged queries.
- [x] Add secret management guidance for Kubernetes and bare metal.

### 2. Retention and Lifecycle

- [x] Add retention classes and delete manifests.
- [x] Add compactor retention enforcement.
- [x] Add legal-hold or protected-tenant hooks if required later.
- [x] Add reindex and backfill operational procedures.

### 3. Observability and Ops

- [x] Add Prometheus metrics across all services.
- [x] Add OpenTelemetry tracing across ingest and query paths.
- [x] Add Grafana dashboards and alerts.
- [x] Add runbooks for WAL replay, Kafka lag, object-store failures, and compaction lag.
- [x] Add capacity-planning guidance for Kubernetes and hybrid deployments.

### 4. Deployment Hardening

- [x] Add production Helm values and topology profiles.
- [x] Add PodDisruptionBudgets, anti-affinity, and autoscaling rules.
- [x] Add persistent-volume guidance for ingesters.
- [x] Add bare-metal reference topology with systemd and host placement guidance.
- [x] Add backup and restore procedures for PostgreSQL and object-store metadata.

### Exit Criteria

- [x] Multi-tenant quotas are enforced.
- [ ] Production deployment docs are usable without repository knowledge.
- [ ] SLO dashboards and core alerts are in place.
- [ ] Security posture is documented and testable.

## Cross-Cutting Gates

### Before Merging Any Major Feature

- [ ] Unit tests cover success and failure paths.
- [ ] Public or internal API changes are reflected in `docs/API.md`.
- [ ] Architecture-impacting decisions are captured in an ADR when needed.
- [ ] Metrics and logs are added for operationally significant behavior.

### Before Calling A Phase Complete

- [ ] The checklist items for that phase are updated.
- [ ] Relevant docs are refreshed.
- [ ] Manual verification steps are written down.
- [ ] Known gaps are recorded in `docs/ROADMAP.md` or follow-up issues.
