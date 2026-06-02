# Log Aggregation System

A greenfield, Loki-inspired log aggregation platform focused on:

- label-based stream discovery
- structured-field indexing
- bounded text search acceleration
- strong multi-tenancy
- Kubernetes and hybrid bare-metal deployment

This repository currently contains:

- implementation scaffold for the core services
- shared domain models and service runtime utilities
- deployment scaffolding for Docker Compose and Helm
- project documentation in [`docs/`](/Users/sanskar/dev/Research/Projects/Log-Aggregation-System/docs)
- single-node usage guide in [docs/SINGLE_NODE.md](/Users/sanskar/dev/Research/Projects/Log-Aggregation-System/docs/SINGLE_NODE.md)

## Services

- `gateway`: external ingest and query entrypoint
- `distributor`: write-path validation and stream routing
- `parser`: log normalization and enrichment pipeline executor
- `ingester`: WAL, chunking, and flush orchestration
- `indexer`: label and field index segment builder
- `query-frontend`: query planning, splitting, and caching
- `querier`: chunk retrieval and query execution
- `compactor`: segment merge, retention, and maintenance
- `control-plane`: tenant config and policy management

## Quick Start

```bash
make fmt
make test
make build
```

Run a single service locally:

```bash
SERVICE_HTTP_ADDR=:8080 go run ./cmd/gateway
```

See [docs/SINGLE_NODE.md](/Users/sanskar/dev/Research/Projects/Log-Aggregation-System/docs/SINGLE_NODE.md) for ingest/query examples and local engine behavior.

## Repository Layout

```text
cmd/                 Service entrypoints
internal/core/       Domain types and service contracts
internal/platform/   Config, runtime, and HTTP scaffolding
internal/services/   Service descriptors and planned endpoints
deploy/              Compose and Helm scaffolding
docs/                Product, architecture, and execution docs
pkg/version/         Build version information
```
