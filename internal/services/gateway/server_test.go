package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/queue"
	"github.com/sanskar/log-aggregation-system/internal/engine/singlenode"
)

func TestNativeIngestAndSearch(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	handler := server.Handler()

	payload, _ := json.Marshal(map[string]any{
		"events": []model.Event{
			{
				Timestamp: time.Now().UTC(),
				TenantID:  "default",
				Body:      `{"service":"checkout","status_code":500,"message":"timeout"}`,
				Severity:  "error",
			},
		},
	})

	ingestReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", bytes.NewReader(payload))
	ingestRes := httptest.NewRecorder()
	handler.ServeHTTP(ingestRes, ingestReq)
	if ingestRes.Code != http.StatusAccepted {
		t.Fatalf("unexpected ingest status: %d", ingestRes.Code)
	}

	searchPayload, _ := json.Marshal(model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "checkout"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextContains:    "timeout",
		Start:           time.Now().Add(-time.Minute),
		End:             time.Now().Add(time.Minute),
		Limit:           10,
	})
	searchReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", bytes.NewReader(searchPayload))
	searchRes := httptest.NewRecorder()
	handler.ServeHTTP(searchRes, searchReq)
	if searchRes.Code != http.StatusOK {
		t.Fatalf("unexpected search status: %d", searchRes.Code)
	}
}

func TestQueuedNativeIngestAndQueueStats(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	broker := queue.NewMemoryBroker(2)
	server := NewQueuedServer(engine, broker)
	handler := server.Handler()
	now := time.Now().UTC()

	payload, _ := json.Marshal(map[string]any{
		"events": []model.Event{
			{
				Timestamp: now,
				TenantID:  "default",
				Body:      `{"service":"checkout","status_code":201,"message":"queued"}`,
			},
		},
	})
	ingestReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", bytes.NewReader(payload))
	ingestRes := httptest.NewRecorder()
	handler.ServeHTTP(ingestRes, ingestReq)
	if ingestRes.Code != http.StatusAccepted {
		t.Fatalf("unexpected queued ingest status: %d", ingestRes.Code)
	}

	searchPayload, _ := json.Marshal(model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "checkout"},
		TextContains:   "queued",
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          10,
	})
	searchReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", bytes.NewReader(searchPayload))
	searchRes := httptest.NewRecorder()
	handler.ServeHTTP(searchRes, searchReq)
	if searchRes.Code != http.StatusOK {
		t.Fatalf("unexpected queued search status: %d", searchRes.Code)
	}

	statsReq := httptest.NewRequest(http.MethodGet, "/api/admin/queue", nil)
	statsRes := httptest.NewRecorder()
	handler.ServeHTTP(statsRes, statsReq)
	if statsRes.Code != http.StatusOK {
		t.Fatalf("unexpected queue stats status: %d", statsRes.Code)
	}
	if !bytes.Contains(statsRes.Body.Bytes(), []byte(queue.TopicNormalizedEvents)) {
		t.Fatalf("expected normalized topic in stats: %s", statsRes.Body.String())
	}

	replayPayload, _ := json.Marshal(map[string]any{
		"topic": queue.TopicNormalizedEvents,
		"limit": 10,
	})
	replayReq := httptest.NewRequest(http.MethodPost, "/api/admin/queue/replay", bytes.NewReader(replayPayload))
	replayRes := httptest.NewRecorder()
	handler.ServeHTTP(replayRes, replayReq)
	if replayRes.Code != http.StatusOK {
		t.Fatalf("unexpected replay status: %d", replayRes.Code)
	}
	if !bytes.Contains(replayRes.Body.Bytes(), []byte(queue.TopicNormalizedEvents)) {
		t.Fatalf("expected replayed normalized topic in response: %s", replayRes.Body.String())
	}
}

func TestNativeExplainAndLokiCompatibility(t *testing.T) {
	engine, err := singlenode.NewWithOptions(t.TempDir(), singlenode.Options{
		ChunkMaxEvents:   1,
		ChunkMaxBytes:    1024,
		ChunkMaxDuration: time.Minute,
		JSONIgnoreError:  true,
		EnableLogfmt:     true,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	handler := server.Handler()
	now := time.Now().UTC()

	for _, body := range []string{
		`{"service":"checkout","status_code":500,"message":"timeout one"}`,
		`{"service":"checkout","status_code":500,"message":"timeout two"}`,
	} {
		payload, _ := json.Marshal(map[string]any{
			"events": []model.Event{{Timestamp: now, TenantID: "default", Body: body, Severity: "error"}},
		})
		ingestReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", bytes.NewReader(payload))
		ingestRes := httptest.NewRecorder()
		handler.ServeHTTP(ingestRes, ingestReq)
		if ingestRes.Code != http.StatusAccepted {
			t.Fatalf("unexpected ingest status: %d", ingestRes.Code)
		}
	}

	explainPayload, _ := json.Marshal(model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "checkout"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextRegex:       `timeout\s+\w+`,
		Start:           now.Add(-time.Minute),
		End:             now.Add(time.Minute),
	})
	explainReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/explain", bytes.NewReader(explainPayload))
	explainRes := httptest.NewRecorder()
	handler.ServeHTTP(explainRes, explainReq)
	if explainRes.Code != http.StatusOK {
		t.Fatalf("unexpected explain status: %d", explainRes.Code)
	}
	if !bytes.Contains(explainRes.Body.Bytes(), []byte(`"candidate_chunks"`)) {
		t.Fatalf("expected explain output, got %s", explainRes.Body.String())
	}

	queryParams := url.Values{}
	queryParams.Set("query", `{service="checkout"} |= "timeout"`)
	queryParams.Set("start", now.Add(-time.Minute).Format(time.RFC3339Nano))
	queryParams.Set("end", now.Add(time.Minute).Format(time.RFC3339Nano))
	queryParams.Set("limit", "1")
	queryReq := httptest.NewRequest(http.MethodGet, "/api/v1/query_range?"+queryParams.Encode(), nil)
	queryRes := httptest.NewRecorder()
	handler.ServeHTTP(queryRes, queryReq)
	if queryRes.Code != http.StatusOK {
		t.Fatalf("unexpected loki range status: %d", queryRes.Code)
	}
	if !bytes.Contains(queryRes.Body.Bytes(), []byte(`"status":"success"`)) {
		t.Fatalf("expected loki compatibility payload, got %s", queryRes.Body.String())
	}
}

func TestNativeSearchPagination(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	handler := server.Handler()
	now := time.Now().UTC()

	for _, body := range []string{
		`{"service":"checkout","status_code":500,"message":"timeout one"}`,
		`{"service":"checkout","status_code":500,"message":"timeout two"}`,
	} {
		payload, _ := json.Marshal(map[string]any{
			"events": []model.Event{{Timestamp: now, TenantID: "default", Body: body, Severity: "error"}},
		})
		ingestReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", bytes.NewReader(payload))
		ingestRes := httptest.NewRecorder()
		handler.ServeHTTP(ingestRes, ingestReq)
		if ingestRes.Code != http.StatusAccepted {
			t.Fatalf("unexpected ingest status: %d", ingestRes.Code)
		}
	}

	searchPayload, _ := json.Marshal(model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "checkout"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextRegex:       `timeout\s+\w+`,
		Start:           now.Add(-time.Minute),
		End:             now.Add(time.Minute),
		Limit:           1,
	})
	searchReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", bytes.NewReader(searchPayload))
	searchRes := httptest.NewRecorder()
	handler.ServeHTTP(searchRes, searchReq)
	if searchRes.Code != http.StatusOK {
		t.Fatalf("unexpected search status: %d", searchRes.Code)
	}

	var first model.QueryResult
	if err := json.Unmarshal(searchRes.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if first.NextCursor == "" {
		t.Fatalf("expected next cursor, got %+v", first)
	}

	secondPayload, _ := json.Marshal(model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "checkout"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextRegex:       `timeout\s+\w+`,
		Start:           now.Add(-time.Minute),
		End:             now.Add(time.Minute),
		Limit:           1,
		Cursor:          first.NextCursor,
	})
	secondReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", bytes.NewReader(secondPayload))
	secondRes := httptest.NewRecorder()
	handler.ServeHTTP(secondRes, secondReq)
	if secondRes.Code != http.StatusOK {
		t.Fatalf("unexpected search status: %d", secondRes.Code)
	}
}

func TestOTLPLogsIngest(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	handler := server.Handler()

	now := time.Now().UTC()
	payload := map[string]any{
		"resourceLogs": []any{
			map[string]any{
				"resource": map[string]any{
					"attributes": []any{
						map[string]any{"key": "tenant_id", "value": map[string]any{"stringValue": "default"}},
					},
				},
				"scopeLogs": []any{
					map[string]any{
						"logRecords": []any{
							map[string]any{
								"timeUnixNano": strconvFormat(now.UnixNano()),
								"severityText": "INFO",
								"body":         map[string]any{"stringValue": `{"service":"api","status_code":200}`},
								"attributes": []any{
									map[string]any{"key": "host", "value": map[string]any{"stringValue": "node-1"}},
								},
							},
						},
					},
				},
			},
		},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/otlp/v1/logs", bytes.NewReader(body))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", res.Code)
	}

	result, err := engine.Query(context.Background(), model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "api"},
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("query engine: %v", err)
	}
	if result.Matched != 1 {
		t.Fatalf("expected 1 match, got %d", result.Matched)
	}
}

func TestLokiPushIngest(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	handler := server.Handler()

	now := time.Now().UTC()
	payload := map[string]any{
		"streams": []any{
			map[string]any{
				"stream": map[string]string{"service": "api", "env": "prod"},
				"values": []any{
					[]string{strconvFormat(now.UnixNano()), "timeout from upstream"},
				},
			},
		},
	}
	body, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewReader(body))
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", res.Code)
	}

	result, err := engine.Query(context.Background(), model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "api"},
		TextContains:   "timeout",
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("query engine: %v", err)
	}
	if result.Matched != 1 {
		t.Fatalf("expected 1 matched event from loki push, got %d", result.Matched)
	}
}

func TestNativeIngestMalformedPayload(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	req := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", bytes.NewBufferString(`{"events":[{"tenant_id":"default"}]}`))
	res := httptest.NewRecorder()

	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", res.Code)
	}
}

func TestNativeSearchMalformedPayload(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	req := httptest.NewRequest(http.MethodPost, "/api/native/v1/search", bytes.NewBufferString(`{"tenant_id":"default","start":"bad"}`))
	res := httptest.NewRecorder()

	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", res.Code)
	}
}

func TestOTLPMalformedPayload(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	req := httptest.NewRequest(http.MethodPost, "/otlp/v1/logs", bytes.NewBufferString(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"abc"}]}]}]}`))
	res := httptest.NewRecorder()

	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", res.Code)
	}
}

func TestLokiMalformedPayload(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewBufferString(`{"streams":[{"stream":{"service":"api"},"values":[["bad","line"]]}]}`))
	res := httptest.NewRecorder()

	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: %d", res.Code)
	}
}

func TestNativeTailAndAdmin(t *testing.T) {
	engine, err := singlenode.New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	server := NewServer(engine)
	handler := server.Handler()
	now := time.Now().UTC()

	for _, body := range []string{
		`{"service":"checkout","status_code":200,"message":"one"}`,
		`{"service":"checkout","status_code":500,"message":"two"}`,
	} {
		payload, _ := json.Marshal(map[string]any{
			"events": []model.Event{{Timestamp: now, TenantID: "default", Body: body}},
		})
		req := httptest.NewRequest(http.MethodPost, "/api/native/v1/ingest", bytes.NewReader(payload))
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != http.StatusAccepted {
			t.Fatalf("unexpected ingest status: %d", res.Code)
		}
		now = now.Add(time.Second)
	}

	tailPayload, _ := json.Marshal(model.TailRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "checkout"},
		Limit:          1,
		Since:          time.Now().Add(-time.Hour),
	})
	tailReq := httptest.NewRequest(http.MethodPost, "/api/native/v1/tail", bytes.NewReader(tailPayload))
	tailRes := httptest.NewRecorder()
	handler.ServeHTTP(tailRes, tailReq)
	if tailRes.Code != http.StatusOK {
		t.Fatalf("unexpected tail status: %d", tailRes.Code)
	}

	adminReq := httptest.NewRequest(http.MethodGet, "/api/admin/tenants", nil)
	adminRes := httptest.NewRecorder()
	handler.ServeHTTP(adminRes, adminReq)
	if adminRes.Code != http.StatusOK {
		t.Fatalf("unexpected admin status: %d", adminRes.Code)
	}
}

func strconvFormat(value int64) string {
	return fmt.Sprintf("%d", value)
}
