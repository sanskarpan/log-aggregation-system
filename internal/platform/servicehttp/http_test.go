package servicehttp

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestInfoEndpoint(t *testing.T) {
	handler := NewHandler(Descriptor{
		Name:    "gateway",
		Role:    "entrypoint",
		Version: "0.1.0-dev",
	})

	req := httptest.NewRequest(http.MethodGet, "/info", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
}

func TestStubEndpoint(t *testing.T) {
	handler := NewHandler(Descriptor{
		Name: "gateway",
		Endpoints: []Endpoint{
			{
				Method:      http.MethodPost,
				Path:        "/otlp/v1/logs",
				State:       "planned",
				Description: "OTLP logs ingest",
			},
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/otlp/v1/logs", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("unexpected status: %d", rec.Code)
	}
}
