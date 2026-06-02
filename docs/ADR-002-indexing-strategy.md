# ADR-002: Hybrid Indexing Strategy

## Status

Accepted

## Decision

Use:

- label index for stream discovery
- structured-field index for tenant-approved metadata
- bloom or token acceleration for bounded text search

Do not globally full-text index every log body in v1.

## Rationale

- strict Loki-only indexing is cheaper but undershoots the search-heavy requirement
- full-search-first indexing would increase storage and operational cost too early
- a hybrid model keeps stream discovery cheap while improving common operational queries

