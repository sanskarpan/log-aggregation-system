package distribution

import (
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestLimiterAllowsAndRejects(t *testing.T) {
	limiter := NewLimiter()
	tenant := model.TenantConfig{
		ID: "tenant-a",
		Limits: model.TenantLimits{
			IngestRateMBPerSecond: 1,
			MaxBodyBytes:          64,
			MaxLabelsPerStream:    2,
		},
	}

	allowed := limiter.Allow(tenant, model.Event{
		TenantID: "tenant-a",
		Body:     "hello",
	})
	if !allowed.Allowed {
		t.Fatalf("expected first event to be allowed, got %+v", allowed)
	}

	rejected := limiter.Allow(tenant, model.Event{
		TenantID: "tenant-a",
		Body:     string(make([]byte, 128)),
	})
	if rejected.Allowed {
		t.Fatal("expected oversize event to be rejected")
	}
	if rejected.Reason == "" {
		t.Fatal("expected rejection reason")
	}

	time.Sleep(10 * time.Millisecond)
}
