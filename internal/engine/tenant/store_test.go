package tenant

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestNewStoreWithDefaults(t *testing.T) {
	store := NewStoreWithDefaults()

	tenant, err := store.GetTenant(context.Background(), "default")
	if err != nil {
		t.Fatalf("expected default tenant, got %v", err)
	}
	if tenant.Limits.MaxLabelsPerStream == 0 {
		t.Fatal("expected tenant limits to be populated")
	}
}

func TestPersistentStoreSnapshotAndReload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "control", "tenants.json")
	store, err := NewPersistentStore(path)
	if err != nil {
		t.Fatalf("new persistent store: %v", err)
	}

	err = store.Put(context.Background(), model.TenantConfig{
		ID:   "tenant-a",
		Name: "Tenant A",
		Limits: model.TenantLimits{
			MaxLabelsPerStream: 8,
			MaxBodyBytes:       1024,
			MaxParsedFields:    16,
			MaxFieldValueBytes: 256,
			Retention:          time.Hour,
		},
	})
	if err != nil {
		t.Fatalf("put tenant: %v", err)
	}

	reloaded, err := NewPersistentStore(path)
	if err != nil {
		t.Fatalf("reload store: %v", err)
	}
	tenant, err := reloaded.GetTenant(context.Background(), "tenant-a")
	if err != nil {
		t.Fatalf("get tenant after reload: %v", err)
	}
	if tenant.Version == 0 {
		t.Fatal("expected persisted version")
	}
}
