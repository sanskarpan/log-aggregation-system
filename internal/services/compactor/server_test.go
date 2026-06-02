package compactor

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
	manifests "github.com/sanskar/log-aggregation-system/internal/storage/manifests"
	objectstore "github.com/sanskar/log-aggregation-system/internal/storage/object"
)

func TestCompactDeletesExpiredManifestsAndChunks(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	objects := objectstore.NewFileStore(root)
	manifestRepo := manifests.NewMemoryRepository()
	tenants := tenant.NewStore()

	tenantCfg := tenant.DefaultTenantConfig()
	tenantCfg.ID = "tenant-a"
	tenantCfg.Limits.Retention = time.Hour
	tenantCfg.Limits.RetentionClass = "warm"
	if err := tenants.Put(ctx, tenantCfg); err != nil {
		t.Fatalf("put tenant: %v", err)
	}

	oldManifestKey := "segments/2026/06/01/old.json"
	oldManifest := persistedSegmentManifest{
		PartitionKey: "2026/06/01/09",
		Start:        time.Now().UTC().Add(-3 * time.Hour),
		End:          time.Now().UTC().Add(-2 * time.Hour),
		GeneratedAt:  time.Now().UTC().Add(-2 * time.Hour),
		Chunks: []persistedChunkRef{
			{
				StreamKey: "tenant-a|service=checkout",
				ObjectKey: "chunks/old-1.zst",
				Checksum:  "abc",
				Start:     time.Now().UTC().Add(-3 * time.Hour),
				End:       time.Now().UTC().Add(-2 * time.Hour),
			},
			{
				StreamKey: "tenant-a|service=checkout",
				ObjectKey: "chunks/old-2.zst",
				Checksum:  "def",
				Start:     time.Now().UTC().Add(-3 * time.Hour),
				End:       time.Now().UTC().Add(-2 * time.Hour),
			},
		},
	}
	oldPayload, _ := json.Marshal(oldManifest)
	if _, err := objects.Put(ctx, oldManifestKey, oldPayload); err != nil {
		t.Fatalf("put old manifest object: %v", err)
	}
	for _, key := range []string{"chunks/old-1.zst", "chunks/old-2.zst"} {
		if _, err := objects.Put(ctx, key, []byte("payload")); err != nil {
			t.Fatalf("put chunk %s: %v", key, err)
		}
	}
	if _, err := manifestRepo.Put(ctx, manifests.Record{
		Key:          oldManifestKey,
		PartitionKey: oldManifest.PartitionKey,
		Start:        oldManifest.Start,
		End:          oldManifest.End,
		Payload:      oldPayload,
		Checksum:     manifests.Checksum(oldPayload),
		CreatedAt:    oldManifest.GeneratedAt,
	}); err != nil {
		t.Fatalf("put old manifest repo: %v", err)
	}

	freshManifestKey := "segments/2026/06/01/fresh.json"
	freshManifest := persistedSegmentManifest{
		PartitionKey: "2026/06/01/09",
		Start:        time.Now().UTC().Add(-10 * time.Minute),
		End:          time.Now().UTC().Add(-5 * time.Minute),
		GeneratedAt:  time.Now().UTC().Add(-5 * time.Minute),
		Chunks: []persistedChunkRef{
			{
				StreamKey: "tenant-a|service=checkout",
				ObjectKey: "chunks/fresh-1.zst",
				Checksum:  "ghi",
				Start:     time.Now().UTC().Add(-10 * time.Minute),
				End:       time.Now().UTC().Add(-5 * time.Minute),
			},
		},
	}
	freshPayload, _ := json.Marshal(freshManifest)
	if _, err := objects.Put(ctx, freshManifestKey, freshPayload); err != nil {
		t.Fatalf("put fresh manifest object: %v", err)
	}
	if _, err := objects.Put(ctx, "chunks/fresh-1.zst", []byte("payload")); err != nil {
		t.Fatalf("put fresh chunk: %v", err)
	}
	if _, err := manifestRepo.Put(ctx, manifests.Record{
		Key:          freshManifestKey,
		PartitionKey: freshManifest.PartitionKey,
		Start:        freshManifest.Start,
		End:          freshManifest.End,
		Payload:      freshPayload,
		Checksum:     manifests.Checksum(freshPayload),
		CreatedAt:    freshManifest.GeneratedAt,
	}); err != nil {
		t.Fatalf("put fresh manifest repo: %v", err)
	}

	server := NewServerWithDeps(tenants, manifestRepo, objects)
	resp, err := server.Compact(ctx, CompactRequest{})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if resp.DeletedManifests != 1 || resp.DeleteManifests != 1 {
		t.Fatalf("unexpected compaction response: %+v", resp)
	}
	if resp.DeletedChunks != 2 {
		t.Fatalf("expected 2 deleted chunks, got %+v", resp)
	}

	if _, _, err := objects.Get(ctx, oldManifestKey); err == nil {
		t.Fatal("expected old manifest object deleted")
	}
	if _, _, err := objects.Get(ctx, "chunks/old-1.zst"); err == nil {
		t.Fatal("expected old chunk deleted")
	}
	if _, err := manifestRepo.Get(ctx, oldManifestKey); err == nil {
		t.Fatal("expected old manifest repo record deleted")
	}
	if _, err := manifestRepo.Get(ctx, freshManifestKey); err != nil {
		t.Fatalf("expected fresh manifest to remain: %v", err)
	}
	deletes, err := manifestRepo.List(ctx, "deletes/")
	if err != nil {
		t.Fatalf("list delete manifests: %v", err)
	}
	if len(deletes) != 1 {
		t.Fatalf("expected one delete manifest, got %+v", deletes)
	}
}

func TestCompactSkipsProtectedTenant(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	objects := objectstore.NewFileStore(root)
	manifestRepo := manifests.NewMemoryRepository()
	tenants := tenant.NewStore()

	protectedTenant := tenant.DefaultTenantConfig()
	protectedTenant.ID = "tenant-b"
	protectedTenant.Protected = true
	protectedTenant.Limits.Retention = time.Hour
	if err := tenants.Put(ctx, protectedTenant); err != nil {
		t.Fatalf("put tenant: %v", err)
	}

	manifestKey := "segments/2026/06/01/protected.json"
	manifest := persistedSegmentManifest{
		PartitionKey: "2026/06/01/09",
		Start:        time.Now().UTC().Add(-3 * time.Hour),
		End:          time.Now().UTC().Add(-2 * time.Hour),
		GeneratedAt:  time.Now().UTC().Add(-2 * time.Hour),
		Chunks: []persistedChunkRef{{
			StreamKey: "tenant-b|service=auth",
			ObjectKey: "chunks/protected.zst",
			Checksum:  "abc",
			Start:     time.Now().UTC().Add(-3 * time.Hour),
			End:       time.Now().UTC().Add(-2 * time.Hour),
		}},
	}
	payload, _ := json.Marshal(manifest)
	if _, err := objects.Put(ctx, manifestKey, payload); err != nil {
		t.Fatalf("put manifest object: %v", err)
	}
	if _, err := objects.Put(ctx, "chunks/protected.zst", []byte("payload")); err != nil {
		t.Fatalf("put chunk: %v", err)
	}
	if _, err := manifestRepo.Put(ctx, manifests.Record{
		Key:          manifestKey,
		PartitionKey: manifest.PartitionKey,
		Start:        manifest.Start,
		End:          manifest.End,
		Payload:      payload,
		Checksum:     manifests.Checksum(payload),
		CreatedAt:    manifest.GeneratedAt,
	}); err != nil {
		t.Fatalf("put manifest repo: %v", err)
	}

	server := NewServerWithDeps(tenants, manifestRepo, objects)
	resp, err := server.Compact(ctx, CompactRequest{})
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if resp.DeletedManifests != 0 {
		t.Fatalf("expected protected tenant to be skipped, got %+v", resp)
	}
	if _, _, err := objects.Get(ctx, manifestKey); err != nil {
		t.Fatalf("expected protected manifest object to remain: %v", err)
	}
}
