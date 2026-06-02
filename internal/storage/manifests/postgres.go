package manifests

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(ctx context.Context, dsn string) (*PostgresRepository, error) {
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
	repo := &PostgresRepository{db: db}
	if err := repo.migrate(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return repo, nil
}

func (r *PostgresRepository) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	return r.db.Close()
}

func (r *PostgresRepository) migrate(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS segment_manifests (
  manifest_key TEXT PRIMARY KEY,
  partition_key TEXT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ NOT NULL,
  checksum TEXT NOT NULL,
  payload BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
)`)
	return err
}

func (r *PostgresRepository) Put(ctx context.Context, record Record) (Record, error) {
	if record.Key == "" {
		return Record{}, errors.New("manifest key is required")
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.Checksum == "" {
		record.Checksum = Checksum(record.Payload)
	}
	existing, err := r.Get(ctx, record.Key)
	if err == nil && existing.Checksum != record.Checksum {
		return Record{}, ErrConflict
	}
	_, err = r.db.ExecContext(ctx, `
INSERT INTO segment_manifests (manifest_key, partition_key, start_time, end_time, checksum, payload, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (manifest_key) DO UPDATE SET
  partition_key = EXCLUDED.partition_key,
  start_time = EXCLUDED.start_time,
  end_time = EXCLUDED.end_time,
  checksum = EXCLUDED.checksum,
  payload = EXCLUDED.payload,
  created_at = EXCLUDED.created_at
`, record.Key, record.PartitionKey, record.Start, record.End, record.Checksum, record.Payload, record.CreatedAt)
	if err != nil {
		return Record{}, err
	}
	return record, nil
}

func (r *PostgresRepository) Get(ctx context.Context, key string) (Record, error) {
	row := r.db.QueryRowContext(ctx, `
SELECT manifest_key, partition_key, start_time, end_time, checksum, payload, created_at
FROM segment_manifests
WHERE manifest_key = $1`, key)
	var record Record
	if err := row.Scan(&record.Key, &record.PartitionKey, &record.Start, &record.End, &record.Checksum, &record.Payload, &record.CreatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, errors.New("manifest not found")
		}
		return Record{}, err
	}
	return record, nil
}

func (r *PostgresRepository) List(ctx context.Context, prefix string) ([]Record, error) {
	query := `
SELECT manifest_key, partition_key, start_time, end_time, checksum, payload, created_at
FROM segment_manifests`
	args := []any{}
	if prefix != "" {
		query += " WHERE manifest_key LIKE $1 || '%'"
		args = append(args, prefix)
	}
	query += " ORDER BY manifest_key"

	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]Record, 0)
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.Key, &record.PartitionKey, &record.Start, &record.End, &record.Checksum, &record.Payload, &record.CreatedAt); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func (r *PostgresRepository) Delete(ctx context.Context, key string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM segment_manifests WHERE manifest_key = $1`, key)
	return err
}
