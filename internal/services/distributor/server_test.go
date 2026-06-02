package distributor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/coordination/ring"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/distribution"
	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
)

func TestDistributorRouteAndRingEndpoints(t *testing.T) {
	backend := ring.NewMemoryBackend()
	coordinator := ring.NewCoordinator(backend, "/logagg/ring", 2)
	server := NewServerWithCoordinator(coordinator, ring.Member{ID: "local", Address: "127.0.0.1:9095", State: ring.StateReady})
	handler := server.Handler()

	registerMember(t, handler, "a", "10.0.0.1:9095")
	registerMember(t, handler, "b", "10.0.0.2:9095")
	registerMember(t, handler, "c", "10.0.0.3:9095")

	payload, _ := json.Marshal(map[string]any{
		"event": model.Event{
			Timestamp:    time.Now().UTC(),
			TenantID:     "default",
			StreamLabels: map[string]string{"service": "payments"},
			Body:         "hello",
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/append", bytes.NewReader(payload))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected route status: %d", res.Code)
	}

	ringReq := httptest.NewRequest(http.MethodGet, "/internal/v1/ring", nil)
	ringRes := httptest.NewRecorder()
	handler.ServeHTTP(ringRes, ringReq)
	if ringRes.Code != http.StatusOK {
		t.Fatalf("unexpected ring status: %d", ringRes.Code)
	}

	assignReq := httptest.NewRequest(http.MethodGet, "/internal/v1/assignment?stream_key=default|service=payments", nil)
	assignRes := httptest.NewRecorder()
	handler.ServeHTTP(assignRes, assignReq)
	if assignRes.Code != http.StatusOK {
		t.Fatalf("unexpected assignment status: %d", assignRes.Code)
	}

	rebalanceReq := httptest.NewRequest(http.MethodGet, "/internal/v1/rebalance?token_count=8", nil)
	rebalanceRes := httptest.NewRecorder()
	handler.ServeHTTP(rebalanceRes, rebalanceReq)
	if rebalanceRes.Code != http.StatusOK {
		t.Fatalf("unexpected rebalance status: %d", rebalanceRes.Code)
	}

	var membersResponse struct {
		Members []ring.Member `json:"members"`
	}
	if err := json.Unmarshal(ringRes.Body.Bytes(), &membersResponse); err != nil {
		t.Fatalf("decode ring response: %v", err)
	}
	if len(membersResponse.Members) < 3 {
		t.Fatalf("expected seeded members in ring, got %+v", membersResponse.Members)
	}

	var assignResponse struct {
		Assignment ring.Assignment `json:"assignment"`
	}
	if err := json.Unmarshal(assignRes.Body.Bytes(), &assignResponse); err != nil {
		t.Fatalf("decode assignment response: %v", err)
	}
	if assignResponse.Assignment.Primary.ID == "" {
		t.Fatal("expected assignment primary")
	}

	primaries := map[string]struct{}{}
	for _, streamKey := range []string{
		"default|service=payments",
		"default|service=orders",
		"default|service=search",
		"default|service=billing",
		"default|service=shipments",
	} {
		assignReq := httptest.NewRequest(http.MethodGet, "/internal/v1/assignment?stream_key="+streamKey, nil)
		assignRes := httptest.NewRecorder()
		handler.ServeHTTP(assignRes, assignReq)
		if assignRes.Code != http.StatusOK {
			t.Fatalf("unexpected assignment status for %s: %d", streamKey, assignRes.Code)
		}
		var result struct {
			Assignment ring.Assignment `json:"assignment"`
		}
		if err := json.Unmarshal(assignRes.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode assignment response: %v", err)
		}
		primaries[result.Assignment.Primary.ID] = struct{}{}
	}
	if len(primaries) < 2 {
		t.Fatalf("expected balanced placements across seeded members, got %+v", primaries)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/internal/v1/members/a", nil)
	deleteRes := httptest.NewRecorder()
	handler.ServeHTTP(deleteRes, deleteReq)
	if deleteRes.Code != http.StatusOK {
		t.Fatalf("unexpected delete status: %d body %s", deleteRes.Code, deleteRes.Body.String())
	}

	failoverPrimaries := map[string]struct{}{}
	for _, streamKey := range []string{
		"default|service=payments",
		"default|service=orders",
		"default|service=search",
	} {
		assignReq := httptest.NewRequest(http.MethodGet, "/internal/v1/assignment?stream_key="+streamKey, nil)
		assignRes := httptest.NewRecorder()
		handler.ServeHTTP(assignRes, assignReq)
		if assignRes.Code != http.StatusOK {
			t.Fatalf("unexpected failover assignment status for %s: %d", streamKey, assignRes.Code)
		}
		var result struct {
			Assignment ring.Assignment `json:"assignment"`
		}
		if err := json.Unmarshal(assignRes.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode failover assignment response: %v", err)
		}
		if result.Assignment.Primary.ID == "a" {
			t.Fatalf("expected deleted member to stop receiving primaries, got %+v", result.Assignment)
		}
		failoverPrimaries[result.Assignment.Primary.ID] = struct{}{}
	}
	if len(failoverPrimaries) == 0 {
		t.Fatal("expected failover primaries")
	}
}

func TestDistributorBackpressureAndAckSemantics(t *testing.T) {
	backend := ring.NewMemoryBackend()
	coordinator := ring.NewCoordinator(backend, "/logagg/ring", 2)
	ctx := httptest.NewRequest(http.MethodGet, "/", nil).Context()
	if _, err := coordinator.Register(ctx, ring.Member{ID: "a", Address: "10.0.0.1:9095", State: ring.StateReady}, 30); err != nil {
		t.Fatalf("register member a: %v", err)
	}

	store := tenant.NewStore()
	if err := store.Put(ctx, model.TenantConfig{
		ID:   "tenant-a",
		Name: "Tenant A",
		Limits: model.TenantLimits{
			IngestRateMBPerSecond: 1,
			MaxLabelsPerStream:    2,
			MaxBodyBytes:          4 * 1024 * 1024,
			MaxParsedFields:       8,
			MaxFieldValueBytes:    16,
			Retention:             time.Hour,
		},
	}); err != nil {
		t.Fatalf("put tenant: %v", err)
	}

	server := NewServerWithDeps(coordinator, ring.Member{ID: "local", Address: "127.0.0.1:9095", State: ring.StateReady}, store, distribution.NewLimiter())
	handler := server.Handler()

	partialPayload, _ := json.Marshal(map[string]any{
		"event": model.Event{
			Timestamp: time.Now().UTC(),
			TenantID:  "tenant-a",
			Body:      "hello",
		},
		"required_acks": 2,
		"strict":        false,
	})
	partialReq := httptest.NewRequest(http.MethodPost, "/internal/v1/append", bytes.NewReader(partialPayload))
	partialRes := httptest.NewRecorder()
	handler.ServeHTTP(partialRes, partialReq)
	if partialRes.Code != http.StatusAccepted {
		t.Fatalf("expected partial ack status, got %d", partialRes.Code)
	}

	var ackResponse map[string]any
	if err := json.Unmarshal(partialRes.Body.Bytes(), &ackResponse); err != nil {
		t.Fatalf("decode partial ack response: %v", err)
	}
	ack := ackResponse["acknowledgment"].(map[string]any)
	if ack["partial_success"] != true {
		t.Fatalf("expected partial success, got %+v", ack)
	}

	oversizePayload, _ := json.Marshal(map[string]any{
		"event": model.Event{
			Timestamp: time.Now().UTC(),
			TenantID:  "tenant-a",
			Body:      strings.Repeat("x", 3*1024*1024),
		},
	})
	oversizeReq := httptest.NewRequest(http.MethodPost, "/internal/v1/append", bytes.NewReader(oversizePayload))
	oversizeRes := httptest.NewRecorder()
	handler.ServeHTTP(oversizeRes, oversizeReq)
	if oversizeRes.Code != http.StatusTooManyRequests {
		t.Fatalf("expected backpressure rejection, got %d", oversizeRes.Code)
	}
}

func registerMember(t *testing.T, handler http.Handler, id, address string) {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"member": ring.Member{
			ID:      id,
			Address: address,
			State:   ring.StateReady,
		},
		"ttl": 30,
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/members", bytes.NewReader(payload))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusCreated {
		t.Fatalf("register member %s: status %d body %s", id, res.Code, res.Body.String())
	}
}
