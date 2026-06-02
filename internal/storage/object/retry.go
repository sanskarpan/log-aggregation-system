package object

import (
	"context"
	"errors"
	"time"
)

type RetryingStore struct {
	inner      Store
	maxRetries int
	backoff    time.Duration
}

func NewRetryingStore(inner Store, maxRetries int, backoff time.Duration) *RetryingStore {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if backoff <= 0 {
		backoff = 50 * time.Millisecond
	}
	return &RetryingStore{
		inner:      inner,
		maxRetries: maxRetries,
		backoff:    backoff,
	}
}

func (s *RetryingStore) Put(ctx context.Context, key string, data []byte) (Metadata, error) {
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		meta, err := s.inner.Put(ctx, key, data)
		if err == nil || errors.Is(err, ErrObjectConflict) {
			return meta, err
		}
		lastErr = err
		if attempt == s.maxRetries {
			break
		}
		if err := sleepWithContext(ctx, s.backoff*time.Duration(attempt+1)); err != nil {
			return Metadata{}, err
		}
	}
	return Metadata{}, lastErr
}

func (s *RetryingStore) Get(ctx context.Context, key string) ([]byte, Metadata, error) {
	return s.inner.Get(ctx, key)
}

func (s *RetryingStore) List(ctx context.Context, prefix string) ([]Metadata, error) {
	return s.inner.List(ctx, prefix)
}

func (s *RetryingStore) Delete(ctx context.Context, key string) error {
	var lastErr error
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		if err := s.inner.Delete(ctx, key); err == nil {
			return nil
		} else {
			lastErr = err
			if attempt == s.maxRetries {
				break
			}
			if err := sleepWithContext(ctx, s.backoff*time.Duration(attempt+1)); err != nil {
				return err
			}
		}
	}
	return lastErr
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
