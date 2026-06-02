package quota

import (
	"context"
	"errors"
	"testing"
)

func TestConcurrencyLimiterTryAcquire(t *testing.T) {
	limiter := NewConcurrencyLimiter()
	release, err := limiter.TryAcquire("tenant-a", 1)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := limiter.TryAcquire("tenant-a", 1); !errors.Is(err, ErrQueryConcurrencyExceeded) {
		t.Fatalf("expected quota error, got %v", err)
	}
	release()
	if _, err := limiter.TryAcquire("tenant-a", 1); err != nil {
		t.Fatalf("expected acquire after release, got %v", err)
	}
}

func TestConcurrencyLimiterWithTenantLimit(t *testing.T) {
	limiter := NewConcurrencyLimiter()
	err := limiter.WithTenantLimit(context.Background(), "tenant-a", 1, func() error {
		_, err := limiter.TryAcquire("tenant-a", 1)
		if !errors.Is(err, ErrQueryConcurrencyExceeded) {
			t.Fatalf("expected quota error inside critical section, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("with tenant limit: %v", err)
	}
}
