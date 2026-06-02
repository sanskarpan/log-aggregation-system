package httpguard

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/platform/authn"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

type Config struct {
	ServiceName   string
	AuthRequired  bool
	Authenticator authn.Authenticator
	AuditPath     string
	ScopesByRoute []ScopeRule
}

type ScopeRule struct {
	Method string
	Path   string
	Prefix string
	Scopes []string
}

type Guard struct {
	cfg       Config
	metrics   *metrics
	auditMu   sync.Mutex
	pathRules []ScopeRule
	buckets   []float64
}

func New(cfg Config) *Guard {
	rules := cfg.ScopesByRoute
	if len(rules) == 0 {
		rules = DefaultScopeRules()
	}
	return &Guard{
		cfg:       cfg,
		metrics:   newMetrics(cfg.ServiceName),
		pathRules: rules,
		buckets:   []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5},
	}
}

func (g *Guard) Wrap(next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			if r.Method != http.MethodGet {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			g.handleMetrics(w)
			return
		}

		start := time.Now()
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		principal, status, message := g.authorize(r)
		if status != 0 {
			servicehttp.WriteJSON(recorder, status, map[string]string{"error": message})
			g.audit(r, recorder.status, time.Since(start), authn.Principal{}, message)
			g.metrics.record(r.Method, r.URL.Path, recorder.status, time.Since(start), g.buckets)
			return
		}

		next.ServeHTTP(recorder, r)
		elapsed := time.Since(start)
		g.audit(r, recorder.status, elapsed, principal, "")
		g.metrics.record(r.Method, r.URL.Path, recorder.status, elapsed, g.buckets)
	})
}

func (g *Guard) handleMetrics(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(g.metrics.render(g.buckets)))
}

func (g *Guard) authorize(r *http.Request) (authn.Principal, int, string) {
	if !g.cfg.AuthRequired {
		return authn.Principal{}, 0, ""
	}
	if g.cfg.Authenticator == nil {
		return authn.Principal{}, http.StatusUnauthorized, "auth is enabled but no authenticator is configured"
	}

	principal, err := g.cfg.Authenticator.Authenticate(r)
	if err != nil {
		return authn.Principal{}, http.StatusUnauthorized, err.Error()
	}

	requiredScopes := g.requiredScopes(r)
	if len(requiredScopes) == 0 {
		return principal, 0, ""
	}
	if !hasScopes(principal.Scopes, requiredScopes) {
		return authn.Principal{}, http.StatusForbidden, fmt.Sprintf("missing required scopes: %s", strings.Join(requiredScopes, ","))
	}
	return principal, 0, ""
}

func (g *Guard) requiredScopes(r *http.Request) []string {
	path := r.URL.Path
	method := r.Method

	switch {
	case path == "/healthz", path == "/readyz", path == "/info":
		return nil
	case path == "/metrics":
		return nil
	case path == "/otlp/v1/logs", path == "/loki/api/v1/push", path == "/api/native/v1/ingest", path == "/internal/v1/append":
		return []string{"ingest:write"}
	case path == "/api/native/v1/search", path == "/api/native/v1/tail", path == "/api/native/v1/explain", path == "/internal/v1/query", path == "/api/v1/query", path == "/api/v1/query_range":
		return []string{"query:read"}
	case strings.HasPrefix(path, "/api/admin/"):
		if method == http.MethodGet {
			return []string{"admin:read"}
		}
		return []string{"admin:write"}
	case strings.HasPrefix(path, "/internal/v1/members"):
		if method == http.MethodGet {
			return []string{"admin:read"}
		}
		return []string{"admin:write"}
	case path == "/internal/v1/ring", path == "/internal/v1/assignment", path == "/internal/v1/rebalance":
		return []string{"admin:read"}
	}

	if strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/internal/") {
		if method == http.MethodGet {
			return []string{"admin:read"}
		}
		return []string{"admin:write"}
	}

	for _, rule := range g.pathRules {
		if rule.Path != "" && path == rule.Path {
			if rule.Method == "" || rule.Method == method {
				return cloneStrings(rule.Scopes)
			}
		}
		if rule.Prefix != "" && strings.HasPrefix(path, rule.Prefix) {
			if rule.Method == "" || rule.Method == method {
				return cloneStrings(rule.Scopes)
			}
		}
	}
	return nil
}

func DefaultScopeRules() []ScopeRule {
	return []ScopeRule{
		{Path: "/api/native/v1/ingest", Method: http.MethodPost, Scopes: []string{"ingest:write"}},
		{Path: "/otlp/v1/logs", Method: http.MethodPost, Scopes: []string{"ingest:write"}},
		{Path: "/loki/api/v1/push", Method: http.MethodPost, Scopes: []string{"ingest:write"}},
		{Path: "/internal/v1/append", Method: http.MethodPost, Scopes: []string{"ingest:write"}},
		{Path: "/api/native/v1/search", Method: http.MethodPost, Scopes: []string{"query:read"}},
		{Path: "/api/native/v1/tail", Method: http.MethodPost, Scopes: []string{"query:read"}},
		{Path: "/api/native/v1/explain", Method: http.MethodPost, Scopes: []string{"query:read"}},
		{Path: "/internal/v1/query", Method: http.MethodPost, Scopes: []string{"query:read"}},
		{Path: "/api/v1/query", Method: http.MethodGet, Scopes: []string{"query:read"}},
		{Path: "/api/v1/query_range", Method: http.MethodGet, Scopes: []string{"query:read"}},
	}
}

func (g *Guard) audit(r *http.Request, status int, elapsed time.Duration, principal authn.Principal, reason string) {
	if strings.TrimSpace(g.cfg.AuditPath) == "" {
		return
	}
	if !g.isPrivilegedPath(r) {
		return
	}
	entry := map[string]any{
		"timestamp":       time.Now().UTC().Format(time.RFC3339Nano),
		"service":         g.cfg.ServiceName,
		"subject":         principal.Subject,
		"method":          r.Method,
		"path":            r.URL.Path,
		"status":          status,
		"duration_ms":     elapsed.Milliseconds(),
		"required_scopes": g.requiredScopes(r),
	}
	if reason != "" {
		entry["reason"] = reason
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return
	}
	g.writeAuditLine(data)
}

func (g *Guard) isPrivilegedPath(r *http.Request) bool {
	if len(g.requiredScopes(r)) > 0 {
		return true
	}
	return strings.HasPrefix(r.URL.Path, "/api/admin/") || strings.HasPrefix(r.URL.Path, "/internal/v1/")
}

func (g *Guard) writeAuditLine(data []byte) {
	g.auditMu.Lock()
	defer g.auditMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(g.cfg.AuditPath), 0o755); err != nil {
		return
	}
	f, err := os.OpenFile(g.cfg.AuditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(data, '\n'))
}

type responseRecorder struct {
	http.ResponseWriter
	status int
}

func (r *responseRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	return r.ResponseWriter.Write(p)
}

type metrics struct {
	service  string
	mu       sync.Mutex
	requests map[string]int64
	hist     map[string]*latencyMetric
}

type latencyMetric struct {
	buckets []int64
	sum     float64
	count   int64
}

func newMetrics(service string) *metrics {
	return &metrics{
		service:  service,
		requests: make(map[string]int64),
		hist:     make(map[string]*latencyMetric),
	}
}

func (m *metrics) record(method, path string, status int, elapsed time.Duration, buckets []float64) {
	key := fmt.Sprintf("%s|%s|%d", method, path, status)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests[key]++
	h := m.hist[fmt.Sprintf("%s|%s|%d", method, path, status)]
	if h == nil {
		h = &latencyMetric{buckets: make([]int64, len(buckets))}
		m.hist[fmt.Sprintf("%s|%s|%d", method, path, status)] = h
	}
	seconds := elapsed.Seconds()
	h.count++
	h.sum += seconds
	for i, upper := range buckets {
		if seconds <= upper {
			h.buckets[i]++
		}
	}
}

func (m *metrics) render(buckets []float64) string {
	m.mu.Lock()
	defer m.mu.Unlock()

	keys := make([]string, 0, len(m.requests))
	for key := range m.requests {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var buf bytes.Buffer
	buf.WriteString("# HELP http_requests_total Total HTTP requests handled by the service.\n")
	buf.WriteString("# TYPE http_requests_total counter\n")
	for _, key := range keys {
		method, path, status := splitMetricKey(key)
		fmt.Fprintf(&buf, "http_requests_total{service=%q,method=%q,path=%q,status=%q} %d\n", m.service, method, path, status, m.requests[key])
	}

	buf.WriteString("# HELP http_request_duration_seconds HTTP request duration histogram.\n")
	buf.WriteString("# TYPE http_request_duration_seconds histogram\n")

	hkeys := make([]string, 0, len(m.hist))
	for key := range m.hist {
		hkeys = append(hkeys, key)
	}
	sort.Strings(hkeys)
	for _, key := range hkeys {
		method, path, status := splitMetricKey(key)
		h := m.hist[key]
		for i, upper := range buckets {
			fmt.Fprintf(&buf, "http_request_duration_seconds_bucket{service=%q,method=%q,path=%q,status=%q,le=%q} %d\n", m.service, method, path, status, formatBucket(upper), h.buckets[i])
		}
		fmt.Fprintf(&buf, "http_request_duration_seconds_bucket{service=%q,method=%q,path=%q,status=%q,le=\"+Inf\"} %d\n", m.service, method, path, status, h.count)
		fmt.Fprintf(&buf, "http_request_duration_seconds_sum{service=%q,method=%q,path=%q,status=%q} %.6f\n", m.service, method, path, status, h.sum)
		fmt.Fprintf(&buf, "http_request_duration_seconds_count{service=%q,method=%q,path=%q,status=%q} %d\n", m.service, method, path, status, h.count)
	}

	return buf.String()
}

func splitMetricKey(key string) (string, string, string) {
	parts := strings.SplitN(key, "|", 3)
	if len(parts) != 3 {
		return "", "", ""
	}
	return parts[0], parts[1], parts[2]
}

func formatBucket(v float64) string {
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", v), "0"), ".")
}

func hasScopes(headerValues []string, required []string) bool {
	if len(required) == 0 {
		return true
	}
	got := map[string]struct{}{}
	for _, header := range headerValues {
		for _, scope := range strings.FieldsFunc(header, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t'
		}) {
			scope = strings.TrimSpace(scope)
			if scope != "" {
				got[scope] = struct{}{}
			}
		}
	}
	for _, scope := range required {
		if _, ok := got[scope]; !ok {
			return false
		}
	}
	return true
}

func cloneStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}
