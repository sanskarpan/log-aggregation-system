# State Repositories And Ring Repair

This project now supports both file-backed and PostgreSQL-backed metadata stores for the control plane and segment manifests.

## Tenant Repository

- Default backend: file snapshot at `DATA_DIR/control/tenants.json`
- PostgreSQL backend: enabled with `TENANT_DSN`
- The repository stores the full `TenantConfig` JSON, including pipeline definitions, searchable fields, and limits.
- The PostgreSQL path is used by the control-plane service and can also be used by data-plane services that need shared tenant state.

## Segment Manifest Repository

- Default backend: memory repository for local development, plus object-store manifests for persistence
- PostgreSQL backend: enabled with `MANIFEST_DSN`
- Each record stores:
  - manifest key
  - partition key
  - time bounds
  - checksum
  - raw manifest payload
  - creation timestamp
- Object storage remains the source of truth for chunk payloads, while the manifest repository gives the system a queryable metadata index for flushes and restores.

## Ring Repair

- `Rebalance` computes deterministic token ownership across ready members.
- `Reconcile` applies an authoritative membership snapshot by upserting current members and removing stale members.
- `MarkDraining` and `MarkLeaving` model join/leave lifecycle transitions explicitly.

## Cache Rules

- Query and chunk caches are invalidated when a flush produces new segment metadata.
- Segment restore hydrates the in-memory segment index from manifest records when available, then falls back to object-store listing.

## Deployment Notes

- `TENANT_DSN` should point to the same PostgreSQL database for control-plane and data-plane services when shared tenant state is desired.
- `MANIFEST_DSN` should point to a PostgreSQL database available to write-path and query-path services that need durable manifest metadata.
- If either DSN is omitted, the repository falls back to local behavior suitable for development.
