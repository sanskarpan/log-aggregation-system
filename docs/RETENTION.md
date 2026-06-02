# Retention And Lifecycle

## Model

- Tenant retention is expressed in `TenantLimits.Retention`.
- Retention classes are labeled with `TenantLimits.RetentionClass` so operators can distinguish hot, warm, and cold policies.
- The compactor enforces retention by scanning immutable segment manifests and deleting expired chunk objects and manifest objects.
- Protected tenants or tenants under legal hold are skipped by compaction until the hold is cleared.

## Delete Manifests

- When a segment expires, the compactor writes a delete manifest under `deletes/<tenant>/...`.
- Delete manifests record the tenant, retention class, original manifest key, removed chunk keys, and deletion timestamp.
- Delete manifests are kept for auditability and later backfill debugging.

## Operational Notes

- Expired manifests are removed from the manifest repository and the object store.
- Chunk deletion is idempotent and safe to retry.
- Retention should be evaluated on a regular compaction schedule, not only manually.
