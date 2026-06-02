package querier

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/quota"
)

type stubQueryEngine struct {
	queryFn func(context.Context, model.QueryRequest) (model.QueryResult, error)
}

func (s *stubQueryEngine) Query(ctx context.Context, req model.QueryRequest) (model.QueryResult, error) {
	if s.queryFn != nil {
		return s.queryFn(ctx, req)
	}
	return model.QueryResult{}, nil
}

func TestHandleExecuteMapsQuotaErrorsTo429(t *testing.T) {
	server := NewServer(&stubQueryEngine{
		queryFn: func(context.Context, model.QueryRequest) (model.QueryResult, error) {
			return model.QueryResult{}, quota.ErrQueryConcurrencyExceeded
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/execute", bytes.NewBufferString(`{"tenant_id":"default"}`))
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", res.Code)
	}
}

func TestHandleExecuteRejectsMalformedPayload(t *testing.T) {
	server := NewServer(&stubQueryEngine{})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/execute", bytes.NewBufferString(`{"tenant_id":`))
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected bad request, got %d", res.Code)
	}
}

func TestHandleExecuteReturnsOk(t *testing.T) {
	server := NewServer(&stubQueryEngine{
		queryFn: func(context.Context, model.QueryRequest) (model.QueryResult, error) {
			return model.QueryResult{Matched: 1}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/execute", bytes.NewBufferString(`{"tenant_id":"default"}`))
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("expected ok, got %d", res.Code)
	}
}
