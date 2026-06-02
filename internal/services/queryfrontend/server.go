package queryfrontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/quota"
	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
	"github.com/sanskar/log-aggregation-system/internal/platform/tlsconfig"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type QueryExecutor interface {
	Execute(context.Context, model.QueryRequest) (model.QueryResult, error)
}

type QueryEngine interface {
	Query(context.Context, model.QueryRequest) (model.QueryResult, error)
	Explain(context.Context, model.QueryRequest) (model.QueryPlan, error)
	CacheStats() map[string]any
}

type Server struct {
	desc    servicehttp.Descriptor
	engine  QueryEngine
	workers []QueryExecutor
}

func NewServer(engine QueryEngine, workers ...QueryExecutor) *Server {
	return &Server{desc: Descriptor(), engine: engine, workers: workers}
}

type HTTPClientOption func(*http.Client)

func WithHTTPClient(client *http.Client) HTTPClientOption {
	return func(dst *http.Client) {
		if client == nil {
			return
		}
		*dst = *client
	}
}

func NewHTTPQuerierClient(baseURL string, opts ...HTTPClientOption) QueryExecutor {
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(client)
		}
	}
	return &httpQuerierClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  client,
		tracer:  otel.Tracer("queryfrontend.http"),
	}
}

func NewHTTPQuerierClientFromConfig(baseURL string, cfg config.Config) (QueryExecutor, error) {
	client, err := tlsconfig.HTTPClient(cfg)
	if err != nil {
		return nil, err
	}
	client.Timeout = 15 * time.Second
	return NewHTTPQuerierClient(baseURL, WithHTTPClient(client)), nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	servicehttp.RegisterBaseRoutes(mux, s.desc)
	mux.HandleFunc("/internal/v1/plan", s.handlePlan)
	mux.HandleFunc("/internal/v1/query", s.handleQuery)
	mux.HandleFunc("/api/admin/cache", s.handleCacheStats)
	return mux
}

func (s *Server) handlePlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req model.QueryRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	plan, err := s.engine.Explain(r.Context(), req)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, plan)
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req model.QueryRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if len(s.workers) == 0 {
		result, err := s.engine.Query(r.Context(), req)
		if err != nil {
			writeQueryError(w, err)
			return
		}
		servicehttp.WriteJSON(w, http.StatusOK, result)
		return
	}

	plan, err := s.engine.Explain(r.Context(), req)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	if len(plan.Fragments) == 0 {
		result, err := s.engine.Query(r.Context(), req)
		if err != nil {
			writeQueryError(w, err)
			return
		}
		servicehttp.WriteJSON(w, http.StatusOK, result)
		return
	}

	merged, err := s.executeFragments(r.Context(), req, plan.Fragments)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, merged)
}

func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"cache": s.engine.CacheStats(),
	})
}

func (s *Server) executeFragments(ctx context.Context, req model.QueryRequest, fragments []model.QueryFragment) (model.QueryResult, error) {
	type fragmentResult struct {
		result model.QueryResult
		err    error
	}
	results := make(chan fragmentResult, len(fragments))
	sem := make(chan struct{}, len(s.workers))
	var wg sync.WaitGroup

	for i, fragment := range fragments {
		wg.Add(1)
		go func(i int, fragment model.QueryFragment) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			worker := s.workers[i%len(s.workers)]
			subReq := req
			subReq.PartitionKey = fragment.PartitionKey
			subReq.Limit = 0
			subReq.Cursor = ""
			result, err := worker.Execute(ctx, subReq)
			results <- fragmentResult{result: result, err: err}
		}(i, fragment)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	merged := model.QueryResult{
		Stats: model.Stats{},
	}
	seen := map[string]struct{}{}
	events := make([]model.Event, 0)
	var firstErr error
	for item := range results {
		if item.err != nil {
			if firstErr == nil {
				firstErr = item.err
			}
			continue
		}
		merged.Scanned += item.result.Scanned
		merged.Matched += item.result.Matched
		merged.Stats.CandidateStreams += item.result.Stats.CandidateStreams
		merged.Stats.CandidateChunks += item.result.Stats.CandidateChunks
		merged.Stats.ScannedEvents += item.result.Stats.ScannedEvents
		for _, event := range item.result.Events {
			key := eventIdentity(event)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			events = append(events, event.Clone())
		}
	}
	if firstErr != nil && len(events) == 0 {
		return model.QueryResult{}, firstErr
	}
	sortEvents(events, req.Tail)
	offset, err := req.CursorOffset()
	if err != nil {
		return model.QueryResult{}, err
	}
	if offset > len(events) {
		offset = len(events)
	}
	remaining := events[offset:]
	if req.Limit > 0 && len(remaining) > req.Limit {
		merged.Events = append(merged.Events, remaining[:req.Limit]...)
		next := offset + len(merged.Events)
		if next < len(events) {
			merged.NextCursor = fmt.Sprintf("%d", next)
		}
	} else {
		merged.Events = append(merged.Events, remaining...)
	}
	merged.Matched = int64(len(merged.Events))
	if firstErr != nil {
		merged.Partial = true
	}
	return merged, nil
}

func writeQueryError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, quota.ErrQueryConcurrencyExceeded) {
		status = http.StatusTooManyRequests
	}
	servicehttp.WriteJSON(w, status, map[string]string{"error": err.Error()})
}

type httpQuerierClient struct {
	baseURL string
	client  *http.Client
	tracer  trace.Tracer
}

func (c *httpQuerierClient) Execute(ctx context.Context, req model.QueryRequest) (model.QueryResult, error) {
	ctx, span := c.tracer.Start(ctx, "querier.execute", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	payload, err := json.Marshal(req)
	if err != nil {
		span.RecordError(err)
		return model.QueryResult{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/internal/v1/execute", bytes.NewReader(payload))
	if err != nil {
		span.RecordError(err)
		return model.QueryResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(httpReq.Header))
	res, err := c.client.Do(httpReq)
	if err != nil {
		span.RecordError(err)
		return model.QueryResult{}, err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		span.RecordError(err)
		return model.QueryResult{}, err
	}
	if res.StatusCode >= http.StatusBadRequest {
		err = fmt.Errorf("querier %s returned %s: %s", c.baseURL, res.Status, strings.TrimSpace(string(data)))
		span.RecordError(err)
		return model.QueryResult{}, err
	}
	var result model.QueryResult
	if err := json.Unmarshal(data, &result); err != nil {
		span.RecordError(err)
		return model.QueryResult{}, err
	}
	return result, nil
}

func decodeJSON(body io.ReadCloser, target any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func sortEvents(events []model.Event, tail bool) {
	sort.SliceStable(events, func(i, j int) bool {
		if tail {
			return events[i].Timestamp.After(events[j].Timestamp)
		}
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
}

func eventIdentity(event model.Event) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s", event.TenantID, event.Timestamp.UTC().Format(time.RFC3339Nano), event.StreamKey(), event.Body, event.TraceID, event.SpanID)
}
