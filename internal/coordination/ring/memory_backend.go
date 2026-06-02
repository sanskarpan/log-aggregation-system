package ring

import (
	"context"
	"errors"
	"sort"
	"sync"
)

type MemoryBackend struct {
	mu      sync.Mutex
	members map[string]Member
	leases  map[int64]struct{}
	nextID  int64
}

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		members: map[string]Member{},
		leases:  map[int64]struct{}{},
		nextID:  100,
	}
}

func (m *MemoryBackend) PutMember(_ context.Context, _ string, member Member, ttl int64) (Member, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ttl > 0 && member.LeaseID == 0 {
		m.nextID++
		member.LeaseID = m.nextID
		m.leases[member.LeaseID] = struct{}{}
	}
	m.members[member.ID] = member
	return member, nil
}

func (m *MemoryBackend) DeleteMember(_ context.Context, _ string, memberID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.members, memberID)
	return nil
}

func (m *MemoryBackend) ListMembers(_ context.Context, _ string) ([]Member, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Member, 0, len(m.members))
	for _, member := range m.members {
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *MemoryBackend) RefreshLease(_ context.Context, leaseID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.leases[leaseID]; !ok {
		return errors.New("lease not found")
	}
	return nil
}

func (m *MemoryBackend) RevokeLease(_ context.Context, leaseID int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.leases, leaseID)
	return nil
}
