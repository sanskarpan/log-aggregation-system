package tenant

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	if dsn == "" {
		return nil, errors.New("dsn is required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(2)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	store := &PostgresStore{db: db}
	if err := store.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *PostgresStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresStore) migrate(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS tenant_configs (
  tenant_id TEXT PRIMARY KEY,
  version BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  config JSONB NOT NULL
)`)
	return err
}

func (s *PostgresStore) Put(ctx context.Context, tenant model.TenantConfig) error {
	if err := tenant.Validate(); err != nil {
		return err
	}
	existingVersion, err := s.versionForTenant(ctx, tenant.ID)
	if err != nil {
		return err
	}
	tenant.Version = existingVersion + 1
	tenant.UpdatedAt = time.Now().UTC()
	payload, err := json.Marshal(tenant)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
INSERT INTO tenant_configs (tenant_id, version, updated_at, config)
VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id) DO UPDATE SET
  version = EXCLUDED.version,
  updated_at = EXCLUDED.updated_at,
  config = EXCLUDED.config
`, tenant.ID, tenant.Version, tenant.UpdatedAt, payload)
	return err
}

func (s *PostgresStore) GetTenant(ctx context.Context, tenantID string) (model.TenantConfig, error) {
	row := s.db.QueryRowContext(ctx, `SELECT config FROM tenant_configs WHERE tenant_id = $1`, tenantID)
	var payload []byte
	if err := row.Scan(&payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.TenantConfig{}, ErrTenantNotFound
		}
		return model.TenantConfig{}, err
	}
	var tenant model.TenantConfig
	if err := json.Unmarshal(payload, &tenant); err != nil {
		return model.TenantConfig{}, err
	}
	return tenant, nil
}

func (s *PostgresStore) List(ctx context.Context) []model.TenantConfig {
	rows, err := s.db.QueryContext(ctx, `SELECT config FROM tenant_configs ORDER BY tenant_id`)
	if err != nil {
		return nil
	}
	defer rows.Close()

	tenants := make([]model.TenantConfig, 0)
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			continue
		}
		var tenant model.TenantConfig
		if err := json.Unmarshal(payload, &tenant); err != nil {
			continue
		}
		tenants = append(tenants, tenant)
	}
	return tenants
}

func (s *PostgresStore) ReloadIfModified(context.Context) (bool, error) {
	return false, nil
}

func (s *PostgresStore) versionForTenant(ctx context.Context, tenantID string) (int64, error) {
	row := s.db.QueryRowContext(ctx, `SELECT version FROM tenant_configs WHERE tenant_id = $1`, tenantID)
	var version int64
	if err := row.Scan(&version); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	return version, nil
}

func (s *PostgresStore) ListTenantIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT tenant_id FROM tenant_configs ORDER BY tenant_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *PostgresStore) String() string {
	return fmt.Sprintf("postgres tenant store %p", s)
}
