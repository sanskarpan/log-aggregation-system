# Data Model

## Canonical Event

The canonical event contains:

- `timestamp`
- `tenant_id`
- `stream_labels`
- `body`
- `severity`
- `resource_attrs`
- `log_attrs`
- `parsed_fields`
- `trace_id`
- `span_id`

This shape is defined in [`internal/core/model/event.go`](/Users/sanskar/dev/Research/Projects/Log-Aggregation-System/internal/core/model/event.go:1).

## Labels

Labels are intended for:

- routing to stream owners
- fast stream discovery
- stable, low-cardinality partitioning

Default label candidates:

- `service`
- `namespace`
- `cluster`
- `env`
- `severity`
- `region`

Never promote unbounded identifiers by default:

- `request_id`
- `user_id`
- `trace_id`
- `pod_uid`
- free-form message fragments

## Structured Fields

Structured fields are parsed metadata retained outside the stream label set.
They are eligible for indexing only when the tenant policy allows them.

Common examples:

- `status_code`
- `host`
- `deployment_id`
- `http_method`
- `component`

## Query Semantics

- label selectors define the candidate stream set
- field predicates intersect approved structured-field postings
- text predicates prune via bloom or token summaries before chunk scan

## Storage Primitives

### Chunk

- tenant-scoped
- stream-fingerprint scoped
- time-bounded
- compressed with `zstd`
- contains chunk metadata, bloom summary, and ordered events

### Index Segment

- immutable
- time-partitioned
- contains label postings, field postings, and chunk references

