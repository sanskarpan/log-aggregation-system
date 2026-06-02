# Product Spec

## Overview

This project is a Loki-inspired log aggregation platform for enterprise deployments that need:

- low-cost label-based discovery
- stronger structured-field search than vanilla Loki
- bounded text search acceleration for recent or selected data
- strong multi-tenant isolation
- first-class Kubernetes support with hybrid bare-metal parity

## Goals

- Ingest logs from Kubernetes, VMs, and platform services through OTel-native interfaces.
- Provide Loki-compatible write and read surfaces where they help Grafana adoption.
- Separate low-cardinality labels from high-cardinality fields to keep indexing sustainable.
- Support per-tenant retention, quotas, parser pipelines, and searchable-field policy.
- Run the same core system on Kubernetes and non-Kubernetes environments.

## Non-Goals

- Global full-text indexing of all retained log bodies in v1.
- Building a custom end-user UI before Grafana integration is stable.
- Replacing upstream collectors in v1.
- Implementing billing or marketplace features in the first milestone.

## Personas

- Platform operator: provisions tenants, retention, auth, and topology.
- Application engineer: searches logs by labels, fields, and recent text patterns.
- SRE: debugs incidents with tail, recent search, and trace correlation.
- Security or compliance team: validates retention and audit boundaries.

## Functional Requirements

### Ingestion

- Accept OTLP logs over HTTP and gRPC.
- Accept Loki-compatible push payloads.
- Resolve tenant identity from auth context and headers.
- Apply parsing and normalization pipelines before durable append.
- Enforce ingestion limits, cardinality budgets, and payload validation.

### Parsing and Enrichment

- Support JSON, logfmt, regex, and static enrichment stages.
- Promote only approved fields to labels.
- Persist non-promoted parsed fields as structured metadata.
- Preserve trace and span correlation fields.

### Storage and Indexing

- Store chunks and immutable index segments in S3-compatible object storage.
- Maintain local WAL for acknowledged-write recovery.
- Build a label index for stream discovery.
- Build structured-field indexes for tenant-approved fields.
- Attach bloom filters to chunks or segments for search pruning.

### Query

- Support time-range queries over label selectors.
- Support field-aware predicates over structured metadata.
- Support bounded text contains or regex search where enabled.
- Support tailing, pagination, and partial-result signaling.

### Control Plane

- CRUD tenants and tenant policies.
- Configure searchable fields, parsing pipelines, and retention classes.
- Publish placement, schema, and policy metadata to data-plane services.

## Non-Functional Requirements

- First-class Kubernetes deployment support.
- Hybrid bare-metal deployment support without Kubernetes-only assumptions.
- Horizontal scale for write path, query path, and indexing path.
- Acknowledged-write durability across node restart via WAL and replication.
- Observable service health, metrics, traces, and audit logs.

## Success Criteria

- Logs from Kubernetes and VM sources are queryable in Grafana through supported APIs.
- Tenant isolation is enforceable for auth, quotas, and visibility.
- Structured-field indexing reduces chunk scan volume for common queries.
- The repository contains enough contracts and scaffolding for incremental implementation passes.

