package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestProcessorJSONAndPromotion(t *testing.T) {
	processor := NewProcessor()
	tenant := model.TenantConfig{
		ID:                  "tenant-a",
		ReservedLabels:      []string{"service", "severity"},
		LabelPromotionAllow: []string{"service", "status_code"},
	}
	event := model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "tenant-a",
		Body:      `{"service":"api","status_code":500}`,
		Severity:  "error",
	}
	pipeline := model.ParsePipeline{
		Stages: []model.PipelineStage{{Name: "json", Type: "json"}},
	}

	got, err := processor.Process(context.Background(), tenant, event, pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.StreamLabels["service"] != "api" {
		t.Fatalf("expected service label, got %+v", got.StreamLabels)
	}
	if got.StreamLabels["severity"] != "error" {
		t.Fatalf("expected severity label, got %+v", got.StreamLabels)
	}
	if got.ParsedFields["status_code"] != "500" {
		t.Fatalf("expected parsed field, got %+v", got.ParsedFields)
	}
}

func TestProcessorRegexStage(t *testing.T) {
	processor := NewProcessor()
	event := model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "tenant-a",
		Body:      "status=500 host=api-17",
	}
	tenant := model.TenantConfig{ID: "tenant-a"}
	pipeline := model.ParsePipeline{
		Stages: []model.PipelineStage{
			{Name: "regex", Type: "regex", Config: map[string]string{"pattern": `status=(?P<status>\d+)`}},
		},
	}

	got, err := processor.Process(context.Background(), tenant, event, pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ParsedFields["status"] != "500" {
		t.Fatalf("expected regex capture, got %+v", got.ParsedFields)
	}
}

func TestProcessorAdvancedStagesAndStats(t *testing.T) {
	processor := NewProcessor()
	event := model.Event{
		Timestamp: time.Now().UTC().Add(-time.Hour),
		TenantID:  "tenant-a",
		Body:      `level=WARNING secret-token status=500 ts=2026-05-27T10:00:00Z`,
	}
	tenant := model.TenantConfig{
		ID:                  "tenant-a",
		ReservedLabels:      []string{"severity"},
		LabelPromotionAllow: []string{"status_code"},
	}
	pipeline := model.ParsePipeline{
		Stages: []model.PipelineStage{
			{Name: "logfmt", Type: "logfmt"},
			{Name: "static", Type: "static", Config: map[string]string{"status_code": "500"}},
			{Name: "rename-status", Type: "rename", Config: map[string]string{"from": "status", "to": "http_status"}},
			{Name: "redact", Type: "redact", Config: map[string]string{"target": "body", "field": "secret-token"}},
			{Name: "severity", Type: "severity", Config: map[string]string{"source": "level"}},
			{Name: "timestamp", Type: "timestamp", Config: map[string]string{"source": "ts", "format": "rfc3339"}},
		},
	}

	got, err := processor.Process(context.Background(), tenant, event, pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Severity != "warn" {
		t.Fatalf("expected normalized severity, got %s", got.Severity)
	}
	if got.Body == event.Body {
		t.Fatal("expected body redaction")
	}
	if got.ParsedFields["http_status"] != "500" {
		t.Fatalf("expected renamed parsed field, got %+v", got.ParsedFields)
	}
	if got.Timestamp.Format(time.RFC3339) != "2026-05-27T10:00:00Z" {
		t.Fatalf("expected timestamp override, got %s", got.Timestamp.Format(time.RFC3339))
	}
}

func TestProcessorDropAndStats(t *testing.T) {
	processor := NewProcessor()
	event := model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "tenant-a",
		Body:      `{"status":"ignore"}`,
	}
	tenant := model.TenantConfig{ID: "tenant-a"}
	pipeline := model.ParsePipeline{
		Stages: []model.PipelineStage{
			{Name: "json", Type: "json"},
			{Name: "drop-ignore", Type: "filter", Config: map[string]string{"field": "status", "value": "ignore", "op": "equals"}},
		},
	}

	got, err := processor.Process(context.Background(), tenant, event, pipeline)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.LogAttrs["_pipeline_drop"] != "true" {
		t.Fatalf("expected drop marker, got %+v", got.LogAttrs)
	}
	stats := processor.Stats()
	if stats.DroppedEvents != 1 {
		t.Fatalf("expected dropped event stats, got %+v", stats)
	}
}
