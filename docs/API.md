# API Contracts

## External Ingest

### Common auth and telemetry

- All HTTP services now expose `GET /metrics` in Prometheus text format.
- When `AUTH_REQUIRED=true`, requests use `AUTH_MODE` to select static-token or OIDC auth.
- `AUTH_MODE=static` requires `Authorization: Bearer <token>` plus `X-Scopes` for scope assertions.
- `AUTH_MODE=oidc` requires `Authorization: Bearer <jwt>` and validates the JWT against `OIDC_ISSUER_URL`, `OIDC_JWKS_URL`, and `OIDC_AUDIENCE`.
- Scope enforcement uses `X-Scopes` in static mode and token claims in OIDC mode, with comma- or space-separated scopes such as `ingest:write`, `query:read`, and `admin:write`.
- `TLS_ENABLED=true` enables server TLS; `TLS_REQUIRE_CLIENT_CERT=true` plus `TLS_CLIENT_CA_FILE` enables service-to-service mTLS.
- `TLS_CLIENT_CERT_FILE`, `TLS_CLIENT_KEY_FILE`, and `TLS_ROOT_CA_FILE` configure outbound TLS for internal HTTP clients.
- `AUDIT_LOG_PATH` enables append-only privileged-request audit logging through the shared HTTP guard.
- Health and readiness endpoints remain open for local orchestration.

### `POST /otlp/v1/logs`

- Purpose: OTLP HTTP ingest for logs
- Auth: bearer token or mTLS-asserted identity
- Tenant source: token claims or explicit tenant header when allowed
- Request body: OTLP `ExportLogsServiceRequest`
- Response: standard OTLP export response

### `POST /loki/api/v1/push`

- Purpose: Grafana and agent compatibility
- Auth: same tenant enforcement as OTLP path
- Request body: Loki push payload
- Response: `204 No Content` on success

## External Query

### `GET /api/v1/query`

- Purpose: Loki-compatible instant query path
- Inputs:
  - `query`
  - `time`
  - `limit`
- Output: normalized compatibility response with `status=success` and `data.resultType=streams`

### `GET /api/v1/query_range`

- Purpose: Loki-compatible range query path
- Inputs:
  - `query`
  - `start`
  - `end`
  - `step`
  - `limit`
- Output: normalized compatibility response with `status=success`, `data.resultType=streams`, and stats metadata

### Native field-aware query endpoint

- Path: `POST /api/native/v1/search`
- Purpose: expose hybrid label, field, and bounded text predicates
- Quota behavior: returns `429 Too Many Requests` when the tenant's query concurrency limit is exceeded
- Request:

```json
{
  "tenant_id": "tenant-a",
  "label_selectors": {
    "service": "payments",
    "env": "prod"
  },
  "field_predicates": {
    "status_code": "500",
    "host": "api-17"
  },
  "text_contains": "timeout",
  "text_regex": "timeout\\s+error",
  "start": "2026-05-26T00:00:00Z",
  "end": "2026-05-26T01:00:00Z",
  "limit": 500,
  "cursor": "0"
}
```

Response additions:

- `stats.candidate_streams`
- `stats.candidate_chunks`
- `stats.scanned_events`
- `next_cursor`

### Native tail endpoint

- Path: `POST /api/native/v1/tail`
- Purpose: return the most recent matching logs first
- Request:

```json
{
  "tenant_id": "default",
  "label_selectors": {"service":"payments"},
  "field_predicates": {"status_code":"500"},
  "limit": 20,
  "since": "2026-05-27T09:55:00Z"
}
```

### Native explain endpoint

- Path: `POST /api/native/v1/explain`
- Purpose: return the planner view for a native query before execution.
- Request body: same as `POST /api/native/v1/search`
- Response fields:

- `request`
- `nodes`
- `candidate_streams`
- `candidate_chunks`
- `estimated_cost`

### Query frontend internal query endpoint

- Path: `POST /internal/v1/query`
- Purpose: plan a query, fan it out to querier workers by fragment, then merge and paginate the merged result.
- Quota behavior: returns `429 Too Many Requests` when the tenant's query concurrency limit is exceeded
- Request body: same as `POST /api/native/v1/search`
- Response: same shape as `model.QueryResult`, including `events`, `scanned`, `matched`, `partial`, `next_cursor`, and `stats`

### Cache stats endpoint

- Path: `GET /api/admin/cache`
- Purpose: expose query and chunk cache hit, miss, eviction, and entry counts.
- Response fields:

- `cache.query.entries`
- `cache.query.hits`
- `cache.query.misses`
- `cache.query.evictions`
- `cache.chunk.entries`
- `cache.chunk.hits`
- `cache.chunk.misses`
- `cache.chunk.evictions`

### Queue stats endpoint

- Path: `GET /api/admin/queue`
- Purpose: expose local broker topic, offset, commit, and lag information.
- Response fields:

- `enabled`
- `topics[].topic`
- `topics[].partitions`
- `topics[].high_watermark`
- `topics[].committed`
- `topics[].lag`

### Queue replay endpoint

- Path: `POST /api/admin/queue/replay`
- Purpose: replay a topic from the beginning for recovery, backfill, or inspection workflows.
- Request body:

```json
{
  "topic": "logs.normalized.v1",
  "limit": 100
}
```

- Response fields:

- `enabled`
- `topic`
- `limit`
- `replayed`

## Runtime and Environment

- `AUTH_REQUIRED=true|false`
- `AUTH_MODE=off|static|oidc`
- `AUTH_BEARER_TOKEN=<static bearer token>`
- `OIDC_ISSUER_URL=<issuer url>`
- `OIDC_JWKS_URL=<jwks url>`
- `OIDC_AUDIENCE=<audience>`
- `OIDC_SCOPES_CLAIM=scope|scp`
- `AUDIT_LOG_PATH=<append-only audit log path>`
- `TLS_ENABLED=true|false`
- `TLS_CERT_FILE=<server certificate>`
- `TLS_KEY_FILE=<server private key>`
- `TLS_CLIENT_CA_FILE=<client CA bundle>`
- `TLS_REQUIRE_CLIENT_CERT=true|false`
- `TLS_CLIENT_CERT_FILE=<client certificate>`
- `TLS_CLIENT_KEY_FILE=<client private key>`
- `TLS_ROOT_CA_FILE=<client root CA bundle>`
- `TLS_SERVER_NAME=<expected server name>`
- `TLS_SKIP_VERIFY=true|false`
- `SERVICE_HTTP_ADDR=:8080`
- `HTTP_READ_TIMEOUT`, `HTTP_WRITE_TIMEOUT`

### Distributor assignment endpoint

- Path: `GET /internal/v1/assignment?stream_key=...`
- Purpose: resolve and inspect the current primary plus replica placement for one stream.
- Response fields:

- `assignment.stream_key`
- `assignment.fingerprint`
- `assignment.primary`
- `assignment.replicas`

### Distributor rebalance endpoint

- Path: `GET /internal/v1/rebalance?token_count=...`
- Purpose: inspect the current token ownership distribution and rebalance report.
- Response fields:

- `ready_members`
- `tokens`
- `primary_count`
- `replica_count`
- `assignment_matrix`

### Distributor member registration endpoint

- Path: `POST /internal/v1/members`
- Purpose: seed or refresh ring members over HTTP so multi-node routing can be exercised in-process and in deployments.
- Request body:

```json
{
  "member": {
    "id": "ingester-1",
    "address": "10.0.0.1:9095",
    "state": "ready"
  },
  "ttl": 30
}
```

- Response fields:

- `member`

### Distributor member mutation endpoint

- Path: `PUT /internal/v1/members/{id}`
- Purpose: transition a member into draining or leaving before removal.
- Request body:

```json
{
  "state": "leaving",
  "ttl": 30
}
```

- Response fields:

- `member`

### Distributor member delete endpoint

- Path: `DELETE /internal/v1/members/{id}`
- Purpose: remove a ring member from the coordinator and trigger reassignment to surviving members.
- Response fields:

- `deleted`

## Control Plane

### `POST /api/admin/tenants`

- Creates a tenant and default policy set.
- Request body: partial or full `TenantConfig` JSON.
- Response: created tenant record.

### `PUT /api/admin/tenants/{id}/limits`

- Updates ingestion, retention, and query limits.
- Request body:

```json
{
  "ingest_rate_mb_per_second": 50,
  "query_concurrency": 8,
  "max_labels_per_stream": 12,
  "max_body_bytes": 262144,
  "max_parsed_fields": 64,
  "max_field_value_bytes": 2048,
  "retention_class": "standard",
  "retention": 604800000000000
}
```

### `PUT /api/admin/tenants/{id}/pipelines`

- Replaces parser pipeline configuration for the tenant.
- Request body:

```json
{
  "pipelines": [
    {
      "id": "default",
      "description": "JSON pipeline",
      "stages": [
        {
          "name": "json",
          "type": "json",
          "config": {
            "source": "body"
          }
        }
      ]
    }
  ],
  "active_pipeline_id": "default"
}
```

### `PUT /api/admin/tenants/{id}/searchable-fields`

- Updates the structured-field indexing allowlist.
- Request body:

```json
{
  "searchable_fields": ["service", "status_code", "host"]
}
```

### `GET /api/admin/tenants`

- Returns locally loaded tenant configs in the file-backed control-plane service.
- Single-node gateway responses may include parser stats and current segment summaries in a later pass.

## Compactor

### `POST /internal/v1/compact`

- Scans manifests, applies tenant retention cutoffs, deletes expired chunk and manifest objects, and writes delete manifests.
- Request body:

```json
{
  "tenant_id": "tenant-a",
  "dry_run": false,
  "partition_key": "2026/06/01/09"
}
```

### `GET /internal/v1/status`

- Returns the compactor data directory and readiness status.

## Internal Service Boundaries

- `POST /internal/v1/append` on distributor: route a canonical event to a primary plus replica set
- `GET /internal/v1/ring` on distributor: inspect current ring snapshot
- `GET /internal/v1/members` on distributor: inspect members and local placement identity
- `GET /internal/v1/assignment` on distributor: inspect placement for a single stream key
- `GET /internal/v1/rebalance` on distributor: inspect token ownership and rebalance reports
- `POST /internal/v1/members` on distributor: register or refresh a ring member
- `PUT /internal/v1/members/{id}` on distributor: update a ring member state
- `DELETE /internal/v1/members/{id}` on distributor: remove a ring member and trigger reassignment
- distributor append contract
- parser process contract
- ingester append contract
- indexer segment build contract
- query-frontend plan contract
- querier execute contract
- query-frontend internal query contract
- cache stats contract
- queue replay contract
- distributor assignment contract
- distributor rebalance contract
- distributor assignment contract
- distributor rebalance contract

These internal APIs should be defined as gRPC in a later pass, but the service roles and paths are now fixed by documentation and descriptors.

Distributor route request:

```json
{
  "event": {
    "timestamp": "2026-05-28T10:00:00Z",
    "tenant_id": "tenant-a",
    "stream_labels": {
      "service": "payments"
    },
    "body": "hello"
  },
  "required_acks": 2,
  "strict": false
}
```

Distributor route response:

- `assignment.primary`
- `assignment.replicas`
- `acknowledgment.required_acks`
- `acknowledgment.available_acks`
- `acknowledgment.partial_success`
- `backpressure.allowed`
- `backpressure.reason`
- `backpressure.retry_after`
