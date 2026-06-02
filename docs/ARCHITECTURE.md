# Architecture

## System Context

The platform is split into:

- data plane: gateway, distributor, parser, ingester, indexer, query-frontend, querier, compactor
- control plane: control-plane service, metadata stores, policy distribution
- infrastructure: Kafka, PostgreSQL, etcd, Redis, and S3-compatible object storage

## End-to-End Flow

1. A client or collector sends OTLP or Loki-compatible logs to `gateway`.
2. `gateway` authenticates, resolves tenant context, and forwards to `distributor`.
3. `distributor` validates payloads, applies limits, and sends events to `parser`.
4. `parser` normalizes records, extracts fields, and applies label-promotion policy.
5. The normalized event is appended to Kafka and routed to an owning `ingester`.
6. `ingester` writes to WAL, appends to in-memory active streams, and flushes compressed chunks.
7. `indexer` builds immutable label and field index segments for flushed data.
8. `compactor` merges small segments, applies retention, and refreshes bloom data.
9. Queries hit `gateway`, then `query-frontend`, which plans work for one or more `querier` instances.
10. `querier` resolves candidate streams and chunks, executes filters, and streams results back.

## Services

### Gateway

- External API entrypoint
- AuthN/AuthZ and tenant resolution
- Compatibility translation for OTLP and Loki endpoints

### Distributor

- Validates labels and tenant budgets
- Performs stream fingerprinting
- Chooses ingester replicas using ring membership

### Parser

- Applies tenant-specific pipelines
- Separates labels, structured fields, and body text
- Produces canonical events

### Ingester

- Owns WAL and active streams
- Rolls chunks by time and size thresholds
- Flushes immutable artifacts to object storage

### Indexer

- Builds time-partitioned label postings
- Builds structured-field indexes for approved keys
- Attaches bloom filters or token summaries

### Query Frontend

- Parses and validates queries
- Splits time ranges and shard work
- Caches plans and merged results

### Querier

- Loads index segments and chunk metadata
- Intersects label and field candidates
- Scans candidate chunks and merges ordered output

### Compactor

- Merges small immutable segments
- Applies retention and deletion manifests
- Rebuilds search acceleration structures

### Control Plane

- Stores tenant policy and parser configs
- Publishes schema and searchable-field definitions
- Tracks jobs, manifests, and operational metadata

## Data Model

- Canonical event shape is defined in `internal/core/model/event.go`.
- Labels are low-cardinality routing and discovery keys.
- Structured fields are filterable metadata that should not explode stream count.
- Text search is acceleration-assisted, not universally pre-indexed.

## Storage Layout

- Local NVMe:
  - WAL segments
  - hot chunk cache
  - hot segment cache
- Object storage:
  - compressed chunks
  - immutable index segments
  - compaction outputs
- PostgreSQL:
  - tenants
  - parser pipelines
  - retention policies
  - manifests and jobs
- etcd:
  - ring membership
  - leases
  - shard ownership metadata
- Kafka:
  - durable ingest buffer
  - retry or replay decoupling

## Deployment Topologies

### Kubernetes

- Stateless services as Deployments
- stateful write-path services can move to StatefulSets when ownership and disk affinity are implemented
- object store can be external or cluster-local
- per-service HPA and PDB policies

### Hybrid Bare Metal

- containerized services under systemd or orchestrated hosts
- explicit etcd and Kafka topology
- object store via MinIO or Ceph-compatible API
- service discovery through etcd-backed ring, not Kubernetes-only service semantics

## Failure Modes

- ingester restart: recover from WAL and replay unflushed streams
- object-store lag: backpressure writes and expose degraded readiness
- index lag: queries fall back to chunk scan within bounded windows
- querier timeout: partial result with shard failure metadata
- control-plane outage: services use cached tenant configs until expiry

