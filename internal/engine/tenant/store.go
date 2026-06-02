package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

var ErrTenantNotFound = errors.New("tenant not found")

type Repository interface {
	Put(context.Context, model.TenantConfig) error
	GetTenant(context.Context, string) (model.TenantConfig, error)
	List(context.Context) []model.TenantConfig
	ReloadIfModified(context.Context) (bool, error)
}

type Store struct {
	mu           sync.RWMutex
	tenants      map[string]model.TenantConfig
	version      int64
	snapshotPath string
	modTime      time.Time
}

type snapshot struct {
	Version int64                `json:"version"`
	Tenants []model.TenantConfig `json:"tenants"`
}

func NewStore() *Store {
	return &Store{
		tenants: map[string]model.TenantConfig{},
	}
}

func NewStoreWithDefaults() *Store {
	store := NewStore()
	defaultTenant := DefaultTenantConfig()
	store.tenants[defaultTenant.ID] = defaultTenant
	store.version = defaultTenant.Version
	return store
}

func NewPersistentStore(snapshotPath string) (*Store, error) {
	store := NewStoreWithDefaults()
	store.snapshotPath = snapshotPath
	if err := store.Reload(context.Background()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return store, nil
}

func (s *Store) Put(_ context.Context, tenant model.TenantConfig) error {
	if err := tenant.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.version++
	tenant.Version = s.version
	tenant.UpdatedAt = time.Now().UTC()
	s.tenants[tenant.ID] = tenant
	return s.saveLocked()
}

func (s *Store) GetTenant(_ context.Context, tenantID string) (model.TenantConfig, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tenant, ok := s.tenants[tenantID]
	if !ok {
		return model.TenantConfig{}, ErrTenantNotFound
	}
	return tenant, nil
}

func (s *Store) List(_ context.Context) []model.TenantConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.tenants))
	for key := range s.tenants {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]model.TenantConfig, 0, len(keys))
	for _, key := range keys {
		out = append(out, s.tenants[key])
	}
	return out
}

func (s *Store) Reload(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.snapshotPath == "" {
		return nil
	}

	data, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		return err
	}

	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	tenants := map[string]model.TenantConfig{}
	for _, tenant := range snap.Tenants {
		if err := tenant.Validate(); err != nil {
			return err
		}
		tenants[tenant.ID] = tenant
	}
	s.tenants = tenants
	s.version = snap.Version
	if info, err := os.Stat(s.snapshotPath); err == nil {
		s.modTime = info.ModTime().UTC()
	}
	return nil
}

func (s *Store) ReloadIfModified(ctx context.Context) (bool, error) {
	if s.snapshotPath == "" {
		return false, nil
	}

	info, err := os.Stat(s.snapshotPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	s.mu.RLock()
	modTime := s.modTime
	s.mu.RUnlock()
	if !info.ModTime().After(modTime) {
		return false, nil
	}

	if err := s.Reload(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) saveLocked() error {
	if s.snapshotPath == "" {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.snapshotPath), 0o755); err != nil {
		return err
	}

	keys := make([]string, 0, len(s.tenants))
	for key := range s.tenants {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	snap := snapshot{Version: s.version, Tenants: make([]model.TenantConfig, 0, len(keys))}
	for _, key := range keys {
		snap.Tenants = append(snap.Tenants, s.tenants[key])
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(s.snapshotPath, data, 0o644); err != nil {
		return err
	}
	if info, err := os.Stat(s.snapshotPath); err == nil {
		s.modTime = info.ModTime().UTC()
	}
	return nil
}

func DefaultTenantConfig() model.TenantConfig {
	return model.TenantConfig{
		ID:                  "default",
		Name:                "Default Tenant",
		Version:             1,
		UpdatedAt:           time.Now().UTC(),
		Protected:           false,
		SearchableFields:    []string{"service", "namespace", "host", "status_code", "component"},
		ReservedLabels:      []string{"service", "namespace", "cluster", "env", "severity", "region"},
		LabelPromotionAllow: []string{"service", "namespace", "cluster", "env", "severity", "region", "host"},
		LabelPromotionDeny:  []string{"request_id", "user_id", "trace_id", "span_id", "pod_uid"},
		Limits: model.TenantLimits{
			IngestRateMBPerSecond: 50,
			QueryConcurrency:      8,
			MaxLabelsPerStream:    12,
			MaxBodyBytes:          256 * 1024,
			MaxParsedFields:       64,
			MaxFieldValueBytes:    2048,
			RetentionClass:        "standard",
			Retention:             7 * 24 * time.Hour,
		},
	}
}
