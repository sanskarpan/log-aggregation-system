package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/queue"
	"github.com/sanskar/log-aggregation-system/internal/engine/quota"
	"github.com/sanskar/log-aggregation-system/internal/engine/singlenode"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

type Server struct {
	desc     servicehttp.Descriptor
	engine   *singlenode.Engine
	broker   queue.Broker
	consumer *queue.Consumer
}

func NewServer(engine *singlenode.Engine) *Server {
	return &Server{
		desc:   Descriptor(),
		engine: engine,
	}
}

func NewQueuedServer(engine *singlenode.Engine, broker queue.Broker) *Server {
	if broker == nil {
		broker = queue.NewMemoryBroker(1)
	}
	server := &Server{
		desc:   Descriptor(),
		engine: engine,
		broker: broker,
	}
	server.consumer = queue.NewConsumer(broker, queue.HandlerFunc(func(ctx context.Context, msg queue.Message) error {
		_, _, err := engine.Append(ctx, msg.Event)
		return err
	}), queue.ConsumerOptions{
		Group:       "gateway-local-writers",
		Topics:      []string{queue.TopicNormalizedEvents, queue.TopicRetryEvents},
		MaxAttempts: 3,
		BatchSize:   100,
	})
	return server
}

func (s *Server) CustomMetrics() []string {
	if s == nil || s.broker == nil {
		return nil
	}
	stats := s.broker.Stats(context.Background())
	if len(stats) == 0 {
		return nil
	}
	lines := []string{
		"# HELP logagg_queue_lag Current queue lag by topic and partition.\n",
		"# TYPE logagg_queue_lag gauge\n",
		"# HELP logagg_queue_committed Current committed offsets by topic and partition.\n",
		"# TYPE logagg_queue_committed gauge\n",
		"# HELP logagg_queue_high_watermark Current high watermark by topic and partition.\n",
		"# TYPE logagg_queue_high_watermark gauge\n",
	}
	for _, stat := range stats {
		highWatermark := int64(0)
		for _, watermark := range stat.HighWatermark {
			highWatermark += watermark
		}
		committed := int64(0)
		for _, value := range stat.Committed {
			committed += value
		}
		lag := highWatermark - committed
		if lag < 0 {
			lag = 0
		}
		lines = append(lines,
			fmt.Sprintf("logagg_queue_lag{service=%q,topic=%q} %d\n", "gateway", stat.Topic, lag),
			fmt.Sprintf("logagg_queue_committed{service=%q,topic=%q} %d\n", "gateway", stat.Topic, committed),
			fmt.Sprintf("logagg_queue_high_watermark{service=%q,topic=%q} %d\n", "gateway", stat.Topic, highWatermark),
		)
	}
	return lines
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	servicehttp.RegisterBaseRoutes(mux, s.desc)

	mux.HandleFunc("/api/native/v1/ingest", s.handleNativeIngest)
	mux.HandleFunc("/api/native/v1/search", s.handleNativeSearch)
	mux.HandleFunc("/api/native/v1/tail", s.handleNativeTail)
	mux.HandleFunc("/api/native/v1/explain", s.handleNativeExplain)
	mux.HandleFunc("/api/admin/tenants", s.handleListTenants)
	mux.HandleFunc("/api/admin/cache", s.handleCacheStats)
	mux.HandleFunc("/api/admin/queue", s.handleQueueStats)
	mux.HandleFunc("/api/admin/queue/replay", s.handleQueueReplay)
	mux.HandleFunc("/otlp/v1/logs", s.handleOTLPLogs)
	mux.HandleFunc("/loki/api/v1/push", s.handleLokiPush)
	mux.HandleFunc("/api/v1/query", s.handleLokiQuery)
	mux.HandleFunc("/api/v1/query_range", s.handleLokiQueryRange)

	return mux
}

type nativeIngestRequest struct {
	Events []model.Event `json:"events"`
}

type nativeIngestResponse struct {
	Accepted int           `json:"accepted"`
	Events   []model.Event `json:"events"`
}

type otlpLogsRequest struct {
	ResourceLogs []otlpResourceLogs `json:"resourceLogs"`
}

type otlpResourceLogs struct {
	Resource struct {
		Attributes []otlpKeyValue `json:"attributes"`
	} `json:"resource"`
	ScopeLogs []otlpScopeLogs `json:"scopeLogs"`
}

type otlpScopeLogs struct {
	LogRecords []otlpLogRecord `json:"logRecords"`
}

type otlpLogRecord struct {
	TimeUnixNano         string         `json:"timeUnixNano"`
	ObservedTimeUnixNano string         `json:"observedTimeUnixNano"`
	SeverityText         string         `json:"severityText"`
	Body                 otlpAnyValue   `json:"body"`
	Attributes           []otlpKeyValue `json:"attributes"`
	TraceID              string         `json:"traceId"`
	SpanID               string         `json:"spanId"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
	IntValue    string `json:"intValue"`
	BoolValue   bool   `json:"boolValue"`
	DoubleValue any    `json:"doubleValue"`
}

type lokiPushRequest struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]string        `json:"values"`
}

func (s *Server) handleNativeIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req nativeIngestRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	events, err := s.appendEvents(r.Context(), req.Events)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, io.EOF) {
			status = http.StatusBadRequest
		}
		servicehttp.WriteJSON(w, status, map[string]string{"error": err.Error()})
		return
	}

	servicehttp.WriteJSON(w, http.StatusAccepted, nativeIngestResponse{
		Accepted: len(events),
		Events:   events,
	})
}

func (s *Server) handleNativeSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req model.QueryRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	result, err := s.engine.Query(r.Context(), req)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) handleNativeExplain(w http.ResponseWriter, r *http.Request) {
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

func (s *Server) handleListTenants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	objects, err := s.engine.ObjectStats(r.Context())
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"tenants":      s.engine.ListTenants(r.Context()),
		"parser_stats": s.engine.ParserStats(),
		"segments":     s.engine.Segments(),
		"objects":      objects,
	})
}

func (s *Server) handleNativeTail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req model.TailRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Limit <= 0 {
		req.Limit = 100
	}

	result, err := s.engine.Tail(r.Context(), req)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, result)
}

func (s *Server) handleLokiQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	req, err := s.parseLokiQueryRequest(r, false)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.engine.Query(r.Context(), req)
	if err != nil {
		writeQueryError(w, err)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "streams",
			"result":     result.Events,
			"stats":      result.Stats,
		},
	})
}

func writeQueryError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, quota.ErrQueryConcurrencyExceeded) {
		status = http.StatusTooManyRequests
	}
	servicehttp.WriteJSON(w, status, map[string]string{"error": err.Error()})
}

func (s *Server) handleLokiQueryRange(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	req, err := s.parseLokiQueryRequest(r, true)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	result, err := s.engine.Query(r.Context(), req)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"status": "success",
		"data": map[string]any{
			"resultType": "streams",
			"result":     result.Events,
			"stats":      result.Stats,
		},
	})
}

func (s *Server) handleQueueStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.broker == nil {
		servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
			"enabled": false,
			"topics":  []queue.TopicStats{},
		})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"enabled": true,
		"topics":  s.broker.Stats(r.Context()),
	})
}

func (s *Server) handleQueueReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if s.broker == nil {
		servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
			"enabled":  false,
			"replayed": []queue.Message{},
		})
		return
	}
	var req struct {
		Topic string `json:"topic"`
		Limit int    `json:"limit"`
	}
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	replayable, ok := s.broker.(queue.ReplayableBroker)
	if !ok {
		servicehttp.WriteJSON(w, http.StatusNotImplemented, map[string]string{"error": "broker does not support replay"})
		return
	}
	messages, err := replayable.Replay(r.Context(), req.Topic, req.Limit)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"enabled":  true,
		"topic":    req.Topic,
		"limit":    req.Limit,
		"replayed": messages,
	})
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

func (s *Server) handleOTLPLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req otlpLogsRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	events := make([]model.Event, 0)
	for _, resourceLogs := range req.ResourceLogs {
		resourceAttrs := attrsToMap(resourceLogs.Resource.Attributes)
		tenantID := firstNonEmpty(resourceAttrs["tenant_id"], r.Header.Get("X-Tenant-ID"), "default")

		for _, scope := range resourceLogs.ScopeLogs {
			for _, record := range scope.LogRecords {
				ts, err := parseOTLPTimestamp(record.TimeUnixNano, record.ObservedTimeUnixNano)
				if err != nil {
					servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
					return
				}
				events = append(events, model.Event{
					Timestamp:     ts,
					TenantID:      tenantID,
					Body:          otlpValueString(record.Body),
					Severity:      record.SeverityText,
					ResourceAttrs: resourceAttrs,
					LogAttrs:      attrsToMap(record.Attributes),
					TraceID:       normalizeHexID(record.TraceID),
					SpanID:        normalizeHexID(record.SpanID),
				})
			}
		}
	}

	appended, err := s.appendEvents(r.Context(), events)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"accepted": len(appended),
		"partialSuccess": map[string]any{
			"rejectedLogRecords": 0,
			"errorMessage":       "",
		},
	})
}

func (s *Server) handleLokiPush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req lokiPushRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	tenantID := firstNonEmpty(r.Header.Get("X-Tenant-ID"), "default")
	events := make([]model.Event, 0)
	for _, stream := range req.Streams {
		for _, value := range stream.Values {
			if len(value) < 2 {
				continue
			}
			ts, err := parseUnixNanoString(value[0])
			if err != nil {
				servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
				return
			}
			event := model.Event{
				Timestamp:    ts,
				TenantID:     tenantID,
				StreamLabels: copyStringMap(stream.Stream),
				Body:         value[1],
			}
			if len(value) > 2 {
				event.LogAttrs = map[string]string{"metadata": value[2]}
			}
			events = append(events, event)
		}
	}

	if _, err := s.appendEvents(r.Context(), events); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) appendEvents(ctx context.Context, events []model.Event) ([]model.Event, error) {
	if s.broker != nil && s.consumer != nil {
		return s.enqueueEvents(ctx, events)
	}

	appended := make([]model.Event, 0, len(events))
	for _, event := range events {
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now().UTC()
		}
		processed, _, err := s.engine.Append(ctx, event)
		if err != nil {
			return nil, err
		}
		if processed.Timestamp.IsZero() && processed.TenantID == "" {
			continue
		}
		appended = append(appended, processed)
	}
	return appended, nil
}

func (s *Server) enqueueEvents(ctx context.Context, events []model.Event) ([]model.Event, error) {
	accepted := make([]model.Event, 0, len(events))
	for _, event := range events {
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now().UTC()
		}
		if event.TenantID == "" {
			event.TenantID = "default"
		}
		if _, err := s.broker.Publish(ctx, queue.PublishRequest{
			Topic: queue.TopicNormalizedEvents,
			Key:   event.StreamKey(),
			Event: event,
		}); err != nil {
			return nil, err
		}
		accepted = append(accepted, event)
	}

	report, err := s.consumer.ConsumeOnce(ctx, len(events))
	if err != nil {
		return nil, err
	}
	if report.DeadLettered > 0 {
		return nil, fmt.Errorf("%d queued events moved to dead-letter topic", report.DeadLettered)
	}
	return accepted, nil
}

func decodeJSON(body io.ReadCloser, target any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func attrsToMap(values []otlpKeyValue) map[string]string {
	out := make(map[string]string, len(values))
	for _, item := range values {
		out[item.Key] = otlpValueString(item.Value)
	}
	return out
}

func otlpValueString(value otlpAnyValue) string {
	switch {
	case value.StringValue != "":
		return value.StringValue
	case value.IntValue != "":
		return value.IntValue
	case value.DoubleValue != nil:
		return fmt.Sprintf("%v", value.DoubleValue)
	case value.BoolValue:
		return "true"
	default:
		return ""
	}
}

func parseOTLPTimestamp(primary, fallback string) (time.Time, error) {
	source := firstNonEmpty(primary, fallback)
	if source == "" {
		return time.Now().UTC(), nil
	}
	return parseUnixNanoString(source)
}

func parseUnixNanoString(value string) (time.Time, error) {
	nanos, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid unix nano timestamp %q", value)
	}
	return time.Unix(0, nanos).UTC(), nil
}

func normalizeHexID(input string) string {
	if input == "" {
		return ""
	}
	if decoded, err := base64.StdEncoding.DecodeString(input); err == nil {
		return fmt.Sprintf("%x", decoded)
	}
	return strings.ToLower(input)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func copyStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func (s *Server) parseLokiQueryRequest(r *http.Request, rangeQuery bool) (model.QueryRequest, error) {
	expr := r.URL.Query().Get("query")
	labels, textContains, textRegex, err := parseLokiExpression(expr)
	if err != nil {
		return model.QueryRequest{}, err
	}
	start, end, err := parseLokiTimeWindow(r, rangeQuery)
	if err != nil {
		return model.QueryRequest{}, err
	}
	limit := parseIntQuery(r.URL.Query().Get("limit"), 100)
	if limit <= 0 {
		limit = 100
	}
	tenantID := firstNonEmpty(r.Header.Get("X-Tenant-ID"), r.URL.Query().Get("tenant"), "default")
	return model.QueryRequest{
		TenantID:       tenantID,
		LabelSelectors: labels,
		TextContains:   textContains,
		TextRegex:      textRegex,
		Start:          start,
		End:            end,
		Limit:          limit,
	}, nil
}

func parseLokiExpression(expr string) (map[string]string, string, string, error) {
	labels := map[string]string{}
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return labels, "", "", nil
	}
	if strings.HasPrefix(expr, "{") {
		end := strings.Index(expr, "}")
		if end < 0 {
			return nil, "", "", fmt.Errorf("invalid loki selector expression")
		}
		selector := strings.TrimSpace(expr[1:end])
		for _, part := range strings.Split(selector, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			pieces := strings.SplitN(part, "=", 2)
			if len(pieces) != 2 {
				return nil, "", "", fmt.Errorf("invalid label selector %q", part)
			}
			key := strings.TrimSpace(pieces[0])
			value := strings.Trim(strings.TrimSpace(pieces[1]), "\"'")
			labels[key] = value
		}
		expr = strings.TrimSpace(expr[end+1:])
	}
	if strings.Contains(expr, "|~") {
		parts := strings.SplitN(expr, "|~", 2)
		return labels, "", strings.Trim(strings.TrimSpace(parts[1]), "\"'"), nil
	}
	if strings.Contains(expr, "|=") {
		parts := strings.SplitN(expr, "|=", 2)
		return labels, strings.Trim(strings.TrimSpace(parts[1]), "\"'"), "", nil
	}
	return labels, "", "", nil
}

func parseLokiTimeWindow(r *http.Request, rangeQuery bool) (time.Time, time.Time, error) {
	q := r.URL.Query()
	if rangeQuery {
		start, err := parseFlexibleTime(q.Get("start"))
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		end, err := parseFlexibleTime(q.Get("end"))
		if err != nil {
			return time.Time{}, time.Time{}, err
		}
		if start.IsZero() || end.IsZero() {
			return time.Time{}, time.Time{}, fmt.Errorf("start and end are required")
		}
		return start, end, nil
	}
	ts, err := parseFlexibleTime(firstNonEmpty(q.Get("time"), q.Get("end")))
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	return ts.Add(-time.Nanosecond), ts.Add(time.Nanosecond), nil
}

func parseFlexibleTime(value string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	if nanos, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(0, nanos).UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return parsed.UTC(), nil
	}
	if parsed, err := time.Parse(time.RFC3339, value); err == nil {
		return parsed.UTC(), nil
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
}

func parseIntQuery(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
