# Operations

## SLO Targets

- ingest availability: `99.9%`
- query success rate: `99.9%`
- recent-data query P95: under `3s` for common tenant-scoped field queries
- acknowledged-write durability: no data loss after local process restart when WAL flush succeeds

## Monitoring

Each service should expose:

- process health and readiness
- request counts and latencies
- request traces via OpenTelemetry when `TRACE_ENABLED=true`
- privileged-request audit logging when `AUDIT_LOG_PATH` is configured
- queue depth, queue lag, and backpressure metrics
- cache hit ratios
- WAL replay duration
- chunk flush and compaction timings
- compactor freshness and last-run duration signals

Operational dashboards should include:

- HTTP request rate and latency
- queue lag by topic and partition
- compactor last-success age

Operational alerts should include:

- `LogAggHighRequestErrorRate`
- `LogAggHighQueryLatency`
- `LogAggHighQueueLag`
- `LogAggStaleCompactorRun`

## Runbooks

Prepare runbooks for:

- ingester restart and WAL replay
- object-store degradation
- Kafka backlog growth
- etcd quorum loss
- PostgreSQL latency or failover
- compaction lag
- stale compactor runs and queue buildup

## Capacity Planning

Track:

- ingest volume per tenant
- active streams per ingester
- index segment growth
- object-store PUT and GET rate
- hot-query cache pressure

## Deployment Targets

- Kubernetes:
  - Helm chart values per environment
  - resource requests and autoscaling profiles
- tracing backend configuration via `TRACE_ENABLED`, `TRACE_EXPORTER`, and OTLP endpoint settings
  - TLS and mTLS configuration via `TLS_ENABLED`, `TLS_CERT_FILE`, `TLS_KEY_FILE`, `TLS_CLIENT_CA_FILE`, and outbound client TLS settings
- Bare metal:
  - service placement guide
  - disk sizing guide for WAL and cache
  - topology recommendations for etcd, Kafka, and object store
- supporting runbooks live in `docs/RUNBOOKS.md`
- secret handling guidance lives in `docs/SECRETS.md`
- backup and restore procedures live in `docs/BACKUP_RESTORE.md`
- deployment hardening guidance lives in `docs/DEPLOYMENT_HARDENING.md`
