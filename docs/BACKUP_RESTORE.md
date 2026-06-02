# Backup And Restore

## PostgreSQL

- Back up tenant and manifest databases with logical dumps or continuous backup tooling.
- Validate restores in a staging namespace before relying on them operationally.
- Treat tenant policy snapshots as reconstructible, but still include them in regular backups.

## Object Storage

- Back up chunk objects, segment manifests, and delete manifests together.
- Preserve object key structure so restore tooling can rebuild segment membership.
- If using S3-compatible storage, enable versioning or immutable backup copies where policy allows.

## Restore Flow

- Restore PostgreSQL metadata first.
- Restore object storage second.
- Restart the query and compactor services after metadata is consistent.
- Rebuild caches by replaying recent WAL and running a low-impact compaction pass.
