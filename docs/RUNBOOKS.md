# Runbooks

## WAL Replay

- Verify the ingester or gateway can read the local WAL directory.
- Check the service logs for replay start and completion.
- Confirm query results after replay before re-enabling write traffic.

## Kafka Backlog

- Inspect broker lag through the queue admin endpoint or Kafka tooling.
- Scale consumers before producers if lag is growing faster than storage pressure.
- Check for poison messages in the retry and dead-letter topics.

## Object Store Failures

- Confirm the object store is reachable and that credentials have not expired.
- Check retry counters and conflict errors.
- If writes are failing, isolate whether the failure is on chunk PUT, manifest PUT, or delete operations.

## Compaction Lag

- Confirm the compactor is running and has access to tenants, manifests, and object storage.
- Inspect delete manifest growth and object store delete failures.
- Run a dry-run compaction if you need to estimate impact before deletion.

## etcd Quorum Loss

- Do not force membership changes while quorum is unavailable.
- Restore quorum before rebalance or delete operations.
- Reconcile stale ownership records after quorum is restored.

## PostgreSQL Latency Or Failover

- Confirm the tenant and manifest repositories are still reachable.
- Watch for snapshot reload failures in the control plane and distributor.
- Retry compaction or control-plane writes only after the database is healthy.

## Reindex And Backfill

- Reindex only after you have validated the target retention and searchable-field configuration.
- Replay the required source data through the ingest path or a controlled backfill job.
- Run compaction after the backfill so delete manifests and retention cutoffs remain consistent.
- Verify query explain output before and after the backfill to confirm the scan profile improved.
