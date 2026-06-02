package quota

import (
	"context"
	"sync"
)

type ConcurrencyLimiter struct {
	mu    sync.Mutex
	gates map[string]chan struct{}
}

func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{gates: map[string]chan struct{}{}}
}

func (l *ConcurrencyLimiter) TryAcquire(tenantID string, limit int) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}

	l.mu.Lock()
	gate, ok := l.gates[tenantID]
	if !ok || cap(gate) != limit {
		gate = make(chan struct{}, limit)
		l.gates[tenantID] = gate
	}
	l.mu.Unlock()

	select {
	case gate <- struct{}{}:
		return func() { <-gate }, nil
	default:
		return nil, ErrQueryConcurrencyExceeded
	}
}

func (l *ConcurrencyLimiter) WithTenantLimit(ctx context.Context, tenantID string, limit int, fn func() error) error {
	release, err := l.TryAcquire(tenantID, limit)
	if err != nil {
		return err
	}
	defer release()
	return fn()
}
