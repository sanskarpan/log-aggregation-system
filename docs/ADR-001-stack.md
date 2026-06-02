# ADR-001: Core Stack

## Status

Accepted

## Decision

Use:

- Go for core services
- Kafka for ingest decoupling
- PostgreSQL for control-plane metadata
- etcd for leases and ring membership
- Redis for query and metadata caching
- S3-compatible object storage for chunks and index segments
- Grafana as the primary UI

## Rationale

- Go matches the concurrency and deployment model used by similar systems.
- Kafka reduces coupling between ingest edges and storage owners.
- PostgreSQL is sufficient for transactional tenant and policy data.
- etcd provides stable distributed coordination for hybrid environments.
- Object storage is the right durability target for immutable chunk and segment data.

