package chunk

import (
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestBuilderFlushAndCodec(t *testing.T) {
	builder := NewBuilderWithOptions("tenant-a|service=api", BuilderOptions{
		MaxEvents:   2,
		MaxBytes:    1024,
		MaxDuration: time.Hour,
	})
	event := model.Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		Body:         "one",
	}
	if flushed, _ := builder.Add(event); flushed {
		t.Fatal("expected first add not to flush")
	}
	flushed, chunk := builder.Add(model.Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		Body:         "two",
	})
	if !flushed || chunk == nil {
		t.Fatal("expected chunk flush")
	}

	encoded, err := Encode(*chunk)
	if err != nil {
		t.Fatalf("encode chunk: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("decode chunk: %v", err)
	}
	if len(decoded.Events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(decoded.Events))
	}
	if decoded.Metadata.EventCount != 2 {
		t.Fatalf("expected metadata event count, got %+v", decoded.Metadata)
	}
}

func TestBuilderFlushByDurationAndBytes(t *testing.T) {
	builder := NewBuilderWithOptions("tenant-a|service=api", BuilderOptions{
		MaxEvents:   10,
		MaxBytes:    100,
		MaxDuration: time.Second,
	})

	first := model.Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		ParsedFields: map[string]string{"status_code": "200"},
		Body:         "short",
	}
	if flushed, _ := builder.Add(first); flushed {
		t.Fatal("expected no flush on first event")
	}

	second := model.Event{
		Timestamp:    first.Timestamp.Add(2 * time.Second),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		ParsedFields: map[string]string{"status_code": "500"},
		Body:         "this event is long enough to force a flush",
	}
	flushed, ch := builder.Add(second)
	if !flushed || ch == nil {
		t.Fatal("expected flush by duration or bytes")
	}
	if ch.Metadata.MinTimestamp.IsZero() || ch.Metadata.MaxTimestamp.IsZero() {
		t.Fatalf("expected chunk metadata timestamps, got %+v", ch.Metadata)
	}
}
