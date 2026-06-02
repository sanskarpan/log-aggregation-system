# Single-Node Guide

This repository now supports a local single-node read/write loop for development and Phase 1 validation.

## What Works

- native ingest via `POST /api/native/v1/ingest`
- native search via `POST /api/native/v1/search`
- native tail via `POST /api/native/v1/tail`
- OTLP HTTP ingest via `POST /otlp/v1/logs`
- Loki-compatible ingest via `POST /loki/api/v1/push`
- tenant and parser status via `GET /api/admin/tenants`
- local WAL replay on restart
- local chunking with event-count, byte, and time-window flush triggers
- in-memory label and structured-field querying with pagination and regex filtering

## Run

```bash
make build
SERVICE_HTTP_ADDR=:8080 DATA_DIR=.data-local go run ./cmd/gateway
```

Useful environment variables:

- `DATA_DIR`
- `CHUNK_MAX_EVENTS`
- `CHUNK_MAX_BYTES`
- `CHUNK_MAX_DURATION`
- `PIPELINE_JSON_IGNORE_ERROR`
- `PIPELINE_ENABLE_LOGFMT`
- `WAL_MAX_SEGMENT_BYTES`
- `WAL_SYNC_MODE`
- `SEGMENT_BUCKET_DURATION`

## Demo Flow

1. Start the gateway.
2. Ingest a native event:

```bash
curl -s http://localhost:8080/api/native/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "events": [
      {
        "timestamp": "2026-05-27T10:00:00Z",
        "tenant_id": "default",
        "body": "{\"service\":\"payments\",\"status_code\":500,\"message\":\"timeout\"}",
        "severity": "error"
      }
    ]
  }'
```

3. Query it:

```bash
curl -s http://localhost:8080/api/native/v1/search \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "default",
    "label_selectors": {"service":"payments"},
    "field_predicates": {"status_code":"500"},
    "text_contains": "timeout",
    "start": "2026-05-27T09:59:00Z",
    "end": "2026-05-27T10:01:00Z",
    "limit": 100
  }'
```

4. Tail recent matching logs:

```bash
curl -s http://localhost:8080/api/native/v1/tail \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "default",
    "label_selectors": {"service":"payments"},
    "limit": 10,
    "since": "2026-05-27T09:55:00Z"
  }'
```

5. Restart the gateway and repeat the query to verify WAL replay.

## Notes

- This is still a single-process, in-memory query/index path. Flushed chunks and segment manifests are now persisted through the local object-store backend under `DATA_DIR/objects`.
- The control-plane tenant store is snapshot-backed on disk under `DATA_DIR/control/tenants.json`.
- Query pagination uses `next_cursor` as an integer offset for now.
- Query services refresh segment manifests from the shared repository before reads, so newly flushed chunks become visible to `gateway`, `query-frontend`, and `querier` without restarting the service.
