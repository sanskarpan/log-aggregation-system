# Object Storage Design

The storage boundary is modeled as an immutable object store. The current implementation uses a local filesystem backend under `DATA_DIR/objects`, while the interface maps cleanly to S3-compatible stores.

## Object Types

### Chunk Payloads

Path shape:

```text
chunks/YYYY/MM/DD/HH/{stream_hash}/{start}-{end}.zst
```

Payload:

- zstd-compressed `chunk.Chunk`
- ordered canonical events
- chunk metadata summaries

### Segment Manifests

Path shape:

```text
segments/{partition_key}/manifests/{generated_at}.json
```

Payload:

- partition key
- partition time range
- chunk object references
- chunk checksums
- chunk metadata summaries

Manifests are versioned instead of overwritten so the file backend can enforce immutable object semantics.

## Idempotency

The local object store computes SHA-256 for every write.

- Same key and same checksum: accepted as an idempotent write.
- Same key and different checksum: rejected as an object conflict.

This matches the behavior expected from immutable object storage layouts.

## Current Runtime Behavior

When the single-node engine flushes a chunk:

1. The chunk is encoded with zstd.
2. The compressed payload is written to object storage.
3. The chunk receives its object key and checksum.
4. The time-partition segment is updated.
5. A new segment manifest is written.

Uploads go through a retry wrapper with bounded backoff so transient filesystem or object-store failures are retried before the write is surfaced to the caller.

Gateway admin state now includes object metadata in:

- `GET /api/admin/tenants`

## Future S3 Adapter

A production adapter should implement `object.Store` with:

- `PutObject` with conditional write or checksum validation
- `GetObject`
- prefix listing
- retry policy for transient failures
- server-side encryption configuration
- object lifecycle and retention policies
