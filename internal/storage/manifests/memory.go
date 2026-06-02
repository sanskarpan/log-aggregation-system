package manifests

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

type MemoryRepository struct {
	mu   sync.RWMutex
	data map[string]Record
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{data: map[string]Record{}}
}

func (m *MemoryRepository) Put(_ context.Context, record Record) (Record, error) {
	if record.Key == "" {
		return Record{}, errors.New("manifest key is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	if record.Checksum == "" {
		record.Checksum = Checksum(record.Payload)
	}
	if existing, ok := m.data[record.Key]; ok && existing.Checksum != record.Checksum {
		return Record{}, ErrConflict
	}
	m.data[record.Key] = record
	return record, nil
}

func (m *MemoryRepository) Get(_ context.Context, key string) (Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	record, ok := m.data[key]
	if !ok {
		return Record{}, errors.New("manifest not found")
	}
	return record, nil
}

func (m *MemoryRepository) List(_ context.Context, prefix string) ([]Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for key := range m.data {
		if prefix == "" || len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	out := make([]Record, 0, len(keys))
	for _, key := range keys {
		out = append(out, m.data[key])
	}
	return out, nil
}

func (m *MemoryRepository) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}
