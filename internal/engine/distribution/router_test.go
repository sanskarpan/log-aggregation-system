package distribution

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/coordination/ring"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestRouterRoute(t *testing.T) {
	backend := ring.NewMemoryBackend()
	coordinator := ring.NewCoordinator(backend, "/ring", 2)
	ctx := context.Background()

	for _, member := range []ring.Member{
		{ID: "a", Address: "10.0.0.1:9095", State: ring.StateReady},
		{ID: "b", Address: "10.0.0.2:9095", State: ring.StateReady},
	} {
		if _, err := coordinator.Register(ctx, member, 30); err != nil {
			t.Fatalf("register member: %v", err)
		}
	}

	router := NewRouter(coordinator)
	result, err := router.Route(ctx, model.Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		Body:         "hello",
	})
	if err != nil {
		t.Fatalf("route event: %v", err)
	}
	if result.Assignment.Primary.ID == "" {
		t.Fatal("expected primary assignment")
	}
	if result.Assignment.StreamKey == "" {
		t.Fatal("expected stream key")
	}
}
