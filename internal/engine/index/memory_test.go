package index

import (
	"context"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestMemoryIndexExecute(t *testing.T) {
	idx := NewMemoryIndex()
	now := time.Now().UTC()
	events := []model.Event{
		{
			Timestamp:    now.Add(-time.Minute),
			TenantID:     "tenant-a",
			StreamLabels: map[string]string{"service": "api", "env": "prod"},
			ParsedFields: map[string]string{"status_code": "500"},
			Body:         "request timeout",
		},
		{
			Timestamp:    now.Add(-30 * time.Second),
			TenantID:     "tenant-a",
			StreamLabels: map[string]string{"service": "api", "env": "prod"},
			ParsedFields: map[string]string{"status_code": "200"},
			Body:         "request success",
		},
	}

	for _, event := range events {
		if err := idx.Index(context.Background(), event); err != nil {
			t.Fatalf("index event: %v", err)
		}
	}

	result, err := idx.Execute(context.Background(), model.QueryRequest{
		TenantID:        "tenant-a",
		LabelSelectors:  map[string]string{"service": "api"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextContains:    "timeout",
		Start:           now.Add(-5 * time.Minute),
		End:             now,
		Limit:           100,
	})
	if err != nil {
		t.Fatalf("execute query: %v", err)
	}
	if result.Matched != 1 {
		t.Fatalf("expected 1 match, got %d", result.Matched)
	}
	if result.Stats.CandidateStreams != 1 {
		t.Fatalf("expected 1 candidate stream, got %d", result.Stats.CandidateStreams)
	}
}

func TestMemoryIndexPaginationAndRegex(t *testing.T) {
	idx := NewMemoryIndex()
	now := time.Now().UTC()
	for _, body := range []string{"timeout alpha", "timeout beta", "success"} {
		if err := idx.Index(context.Background(), model.Event{
			Timestamp:    now,
			TenantID:     "tenant-a",
			StreamLabels: map[string]string{"service": "api"},
			Body:         body,
		}); err != nil {
			t.Fatalf("index event: %v", err)
		}
	}

	first, err := idx.Execute(context.Background(), model.QueryRequest{
		TenantID:       "tenant-a",
		LabelSelectors: map[string]string{"service": "api"},
		TextRegex:      `timeout\s+\w+`,
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          1,
	})
	if err != nil {
		t.Fatalf("execute first page: %v", err)
	}
	if len(first.Events) != 1 || first.NextCursor == "" {
		t.Fatalf("expected first page and next cursor, got %+v", first)
	}

	second, err := idx.Execute(context.Background(), model.QueryRequest{
		TenantID:       "tenant-a",
		LabelSelectors: map[string]string{"service": "api"},
		TextRegex:      `timeout\s+\w+`,
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          1,
		Cursor:         first.NextCursor,
	})
	if err != nil {
		t.Fatalf("execute second page: %v", err)
	}
	if len(second.Events) != 1 {
		t.Fatalf("expected second page, got %+v", second)
	}
}
