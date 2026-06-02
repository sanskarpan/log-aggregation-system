package model

import (
	"testing"
	"time"
)

func TestEventValidate(t *testing.T) {
	event := Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		Body:         "hello",
	}

	if err := event.Validate(); err != nil {
		t.Fatalf("expected valid event, got %v", err)
	}
}

func TestEventStreamKeyStable(t *testing.T) {
	event := Event{
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"env": "prod", "service": "api"},
	}

	if got := event.StreamKey(); got != "tenant-a|env=prod|service=api" {
		t.Fatalf("unexpected stream key: %s", got)
	}
}

func TestEventValidateWithLimits(t *testing.T) {
	event := Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api", "env": "prod"},
		ParsedFields: map[string]string{"status": "500"},
		Body:         "hello",
	}

	err := event.ValidateWithLimits(TenantLimits{
		MaxLabelsPerStream: 1,
		MaxBodyBytes:       100,
		MaxParsedFields:    10,
		MaxFieldValueBytes: 20,
	})
	if err == nil {
		t.Fatal("expected limit error")
	}
}
