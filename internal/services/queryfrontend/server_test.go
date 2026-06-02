package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/quota"
	"github.com/sanskar/log-aggregation-system/internal/engine/singlenode"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type stubExecutor struct {
	calls int
	reqs  []model.QueryRequest
	res   model.QueryResult
}

func (s *stubExecutor) Execute(_ context.Context, req model.QueryRequest) (model.QueryResult, error) {
	s.calls++
	s.reqs = append(s.reqs, req)
	return s.res, nil
}

type stubQueryEngine struct {
	queryFn   func(context.Context, model.QueryRequest) (model.QueryResult, error)
	explainFn func(context.Context, model.QueryRequest) (model.QueryPlan, error)
}

func (s *stubQueryEngine) Query(ctx context.Context, req model.QueryRequest) (model.QueryResult, error) {
	if s.queryFn != nil {
		return s.queryFn(ctx, req)
	}
	return model.QueryResult{}, nil
}

func (s *stubQueryEngine) Explain(ctx context.Context, req model.QueryRequest) (model.QueryPlan, error) {
	if s.explainFn != nil {
		return s.explainFn(ctx, req)
	}
	return model.QueryPlan{}, nil
}

func (s *stubQueryEngine) CacheStats() map[string]any { return map[string]any{} }

func TestHandleQueryFanfoutAndCacheStats(t *testing.T) {
	engine, err := singlenode.NewWithOptions(t.TempDir(), singlenode.Options{
		ChunkMaxEvents:   1,
		ChunkMaxBytes:    1024,
		ChunkMaxDuration: time.Minute,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	now := time.Now().UTC()
	if _, _, err := engine.Append(context.Background(), model.Event{
		Timestamp: now,
		TenantID:  "default",
		StreamLabels: map[string]string{
			"service": "checkout",
		},
		Body: `{"service":"checkout","message":"timeout on checkout"}`,
	}); err != nil {
		t.Fatalf("append event: %v", err)
	}

	stub := &stubExecutor{
		res: model.QueryResult{
			Events: []model.Event{{
				Timestamp: now,
				TenantID:  "default",
				StreamLabels: map[string]string{
					"service": "checkout",
				},
				Body: `{"service":"checkout","message":"timeout on checkout"}`,
			}},
		},
	}
	server := NewServer(engine, stub)
	handler := server.Handler()

	payload, _ := json.Marshal(model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "checkout"},
		TextContains:   "timeout",
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          10,
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/query", bytes.NewReader(payload))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected query status: %d", res.Code)
	}
	if stub.calls == 0 {
		t.Fatal("expected worker to be called")
	}
	if len(stub.reqs) == 0 || stub.reqs[0].PartitionKey == "" {
		t.Fatalf("expected partition key to be set on worker request: %+v", stub.reqs)
	}
	if !bytes.Contains(res.Body.Bytes(), []byte(`timeout on checkout`)) {
		t.Fatalf("expected merged query result, got %s", res.Body.String())
	}

	cacheReq := httptest.NewRequest(http.MethodGet, "/api/admin/cache", nil)
	cacheRes := httptest.NewRecorder()
	handler.ServeHTTP(cacheRes, cacheReq)
	if cacheRes.Code != http.StatusOK {
		t.Fatalf("unexpected cache status: %d", cacheRes.Code)
	}
	if !bytes.Contains(cacheRes.Body.Bytes(), []byte(`"query"`)) {
		t.Fatalf("expected cache stats in response: %s", cacheRes.Body.String())
	}
}

func TestHandleQueryMapsQuotaErrorsTo429(t *testing.T) {
	engine := &stubQueryEngine{
		queryFn: func(context.Context, model.QueryRequest) (model.QueryResult, error) {
			return model.QueryResult{}, quota.ErrQueryConcurrencyExceeded
		},
		explainFn: func(context.Context, model.QueryRequest) (model.QueryPlan, error) {
			return model.QueryPlan{}, quota.ErrQueryConcurrencyExceeded
		},
	}
	server := NewServer(engine)
	handler := server.Handler()

	payload, _ := json.Marshal(model.QueryRequest{
		TenantID: "default",
		Start:    time.Now().Add(-time.Minute),
		End:      time.Now().Add(time.Minute),
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/query", bytes.NewReader(payload))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusTooManyRequests {
		t.Fatalf("expected query 429, got %d", res.Code)
	}

	planReq := httptest.NewRequest(http.MethodPost, "/internal/v1/plan", bytes.NewReader(payload))
	planRes := httptest.NewRecorder()
	handler.ServeHTTP(planRes, planReq)
	if planRes.Code != http.StatusTooManyRequests {
		t.Fatalf("expected plan 429, got %d", planRes.Code)
	}
}

func TestHTTPQuerierClientInjectsTraceContext(t *testing.T) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var seenTraceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenTraceparent = r.Header.Get("traceparent")
		_ = json.NewEncoder(w).Encode(model.QueryResult{})
	}))
	defer server.Close()

	client := NewHTTPQuerierClient(server.URL)
	_, err := client.Execute(context.Background(), model.QueryRequest{TenantID: "default"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if seenTraceparent == "" {
		t.Fatal("expected traceparent header to be injected")
	}
}
