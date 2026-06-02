package httpguard

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/sanskar/log-aggregation-system/internal/platform/authn"
)

func TestGuardExposesMetrics(t *testing.T) {
	guard := New(Config{ServiceName: "gateway"})
	handler := guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/native/v1/search" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", nil)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusOK {
			t.Fatalf("unexpected search status: %d", res.Code)
		}
	}

	metricsReq := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRes := httptest.NewRecorder()
	handler.ServeHTTP(metricsRes, metricsReq)
	if metricsRes.Code != http.StatusOK {
		t.Fatalf("unexpected metrics status: %d", metricsRes.Code)
	}
	body := metricsRes.Body.String()
	if !strings.Contains(body, `http_requests_total{service="gateway",method="POST",path="/api/native/v1/search",status="200"} 2`) {
		t.Fatalf("expected request metric in output, got %s", body)
	}
	if !strings.Contains(body, `http_request_duration_seconds_bucket{service="gateway",method="POST",path="/api/native/v1/search",status="200",le="0.005"}`) {
		t.Fatalf("expected duration buckets in output, got %s", body)
	}
}

func TestGuardAuthAndScopes(t *testing.T) {
	guard := New(Config{
		ServiceName:   "gateway",
		AuthRequired:  true,
		Authenticator: authn.NewStaticTokenAuthenticator("secret-token"),
	})
	handler := guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	unauthReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", nil)
	unauthRes := httptest.NewRecorder()
	handler.ServeHTTP(unauthRes, unauthReq)
	if unauthRes.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized, got %d", unauthRes.Code)
	}

	wrongTokenReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", nil)
	wrongTokenReq.Header.Set("Authorization", "Bearer wrong")
	wrongTokenRes := httptest.NewRecorder()
	handler.ServeHTTP(wrongTokenRes, wrongTokenReq)
	if wrongTokenRes.Code != http.StatusUnauthorized {
		t.Fatalf("expected unauthorized for wrong token, got %d", wrongTokenRes.Code)
	}

	missingScopeReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", nil)
	missingScopeReq.Header.Set("Authorization", "Bearer secret-token")
	missingScopeRes := httptest.NewRecorder()
	handler.ServeHTTP(missingScopeRes, missingScopeReq)
	if missingScopeRes.Code != http.StatusForbidden {
		t.Fatalf("expected forbidden for missing scope, got %d", missingScopeRes.Code)
	}

	allowedReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", nil)
	allowedReq.Header.Set("Authorization", "Bearer secret-token")
	allowedReq.Header.Set("X-Scopes", "ingest:write,query:read")
	allowedRes := httptest.NewRecorder()
	handler.ServeHTTP(allowedRes, allowedReq)
	if allowedRes.Code != http.StatusNoContent {
		t.Fatalf("expected allowed request, got %d", allowedRes.Code)
	}
}

func TestGuardWritesAuditLogForPrivilegedRequests(t *testing.T) {
	dir := t.TempDir()
	auditPath := dir + "/audit.log"
	guard := New(Config{
		ServiceName:   "gateway",
		AuthRequired:  true,
		Authenticator: authn.NewStaticTokenAuthenticator("secret-token"),
		AuditPath:     auditPath,
	})
	handler := guard.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", nil)
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("X-Scopes", "query:read")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.Code)
	}

	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	body := string(data)
	if !strings.Contains(body, `"path":"/api/native/v1/search"`) || !strings.Contains(body, `"subject":"static-token"`) {
		t.Fatalf("expected audit log entry, got %s", body)
	}
}
