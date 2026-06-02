package singlenode

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/cache"
)

func TestEngineAppendQueryAndReplay(t *testing.T) {
	root := t.TempDir()
	engine, err := New(root)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	now := time.Now().UTC().Add(-time.Minute)
	_, _, err = engine.Append(context.Background(), model.Event{
		Timestamp: now,
		TenantID:  "default",
		Body:      `{"service":"payments","status_code":500,"host":"api-17","message":"timeout"}`,
		Severity:  "error",
	})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}

	result, err := engine.Query(context.Background(), model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "payments"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextContains:    "timeout",
		Start:           now.Add(-time.Minute),
		End:             now.Add(time.Minute),
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	if result.Matched != 1 {
		t.Fatalf("expected 1 match, got %d", result.Matched)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	restarted, err := New(root)
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	defer restarted.Close()

	replayed, err := restarted.Replay(context.Background())
	if err != nil {
		t.Fatalf("replay events: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 replayed event, got %d", len(replayed))
	}

	replayedAgain, err := restarted.Replay(context.Background())
	if err != nil {
		t.Fatalf("replay events second time: %v", err)
	}
	if len(replayedAgain) != 0 {
		t.Fatalf("expected idempotent replay on second call, got %d events", len(replayedAgain))
	}

	resultAfterRestart, err := restarted.Query(context.Background(), model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "payments"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextContains:    "timeout",
		Start:           now.Add(-time.Minute),
		End:             now.Add(time.Minute),
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("query after restart: %v", err)
	}
	if resultAfterRestart.Matched != 1 {
		t.Fatalf("expected 1 match after restart, got %d", resultAfterRestart.Matched)
	}
}

func TestEngineAppendRespectsLimits(t *testing.T) {
	engine, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	_, _, err = engine.Append(context.Background(), model.Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "default",
		StreamLabels: map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6", "g": "7", "h": "8", "i": "9", "j": "10", "k": "11", "l": "12", "m": "13"},
		Body:         "too many labels",
	})
	if err == nil {
		t.Fatal("expected limit error")
	}
}

func TestEngineChunkOptions(t *testing.T) {
	engine, err := NewWithOptions(t.TempDir(), Options{
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

	_, flushed, err := engine.Append(context.Background(), model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "default",
		Body:      `{"service":"payments","status_code":200}`,
	})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}
	if flushed == nil {
		t.Fatal("expected flush with chunk max events 1")
	}
	if len(engine.Segments()) != 1 {
		t.Fatalf("expected segment record, got %d", len(engine.Segments()))
	}

	objects, err := engine.ObjectStats(context.Background())
	if err != nil {
		t.Fatalf("list object stats: %v", err)
	}
	var chunkObjects int
	var manifestObjects int
	for _, object := range objects {
		if strings.HasPrefix(object.Key, "chunks/") {
			chunkObjects++
		}
		if strings.HasPrefix(object.Key, "segments/") {
			manifestObjects++
		}
	}
	if chunkObjects != 1 || manifestObjects != 1 {
		t.Fatalf("expected flushed chunk and segment manifest objects, got %+v", objects)
	}
}

func TestEngineTail(t *testing.T) {
	engine, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer engine.Close()

	now := time.Now().UTC().Add(-time.Minute)
	for _, body := range []string{
		`{"service":"payments","status_code":200,"message":"old"}`,
		`{"service":"payments","status_code":500,"message":"new"}`,
	} {
		_, _, err := engine.Append(context.Background(), model.Event{
			Timestamp: now,
			TenantID:  "default",
			Body:      body,
		})
		if err != nil {
			t.Fatalf("append event: %v", err)
		}
		now = now.Add(time.Second)
	}

	result, err := engine.Tail(context.Background(), model.TailRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "payments"},
		Limit:          1,
		Since:          time.Now().UTC().Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("tail events: %v", err)
	}
	if len(result.Events) != 1 {
		t.Fatalf("expected 1 tailed event, got %d", len(result.Events))
	}
	if result.Events[0].ParsedFields["message"] != "new" {
		t.Fatalf("expected newest event first, got %+v", result.Events[0])
	}
}

func TestEngineExplainAndRestoreFromObjects(t *testing.T) {
	root := t.TempDir()
	engine, err := NewWithOptions(root, Options{
		ChunkMaxEvents:     1,
		ChunkMaxBytes:      1024,
		ChunkMaxDuration:   time.Minute,
		JSONIgnoreError:    true,
		EnableLogfmt:       true,
		WALMaxSegmentBytes: 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	now := time.Now().UTC()
	_, _, err = engine.Append(context.Background(), model.Event{
		Timestamp: now,
		TenantID:  "default",
		Body:      `{"service":"payments","status_code":500,"message":"timeout"}`,
	})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}

	plan, err := engine.Explain(context.Background(), model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "payments"},
		TextContains:   "timeout",
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("explain query: %v", err)
	}
	if plan.CandidateChunks == 0 {
		t.Fatalf("expected candidate chunks in explain plan: %+v", plan)
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}

	restarted, err := New(root)
	if err != nil {
		t.Fatalf("restart engine: %v", err)
	}
	defer restarted.Close()
	if _, err := restarted.Replay(context.Background()); err != nil {
		t.Fatalf("replay engine: %v", err)
	}

	result, err := restarted.Query(context.Background(), model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "payments"},
		TextContains:   "timeout",
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("query after restore: %v", err)
	}
	if result.Matched != 1 {
		t.Fatalf("expected restored query to match 1, got %d", result.Matched)
	}

	stats := restarted.CacheStats()
	chunkStats, ok := stats["chunk"].(cache.ChunkStats)
	if !ok {
		t.Fatalf("expected chunk cache stats, got %+v", stats)
	}
	if chunkStats.Entries == 0 {
		t.Fatalf("expected warmed chunk cache entries, got %+v", chunkStats)
	}
}

func TestEngineRefreshesSegmentsWithoutRestart(t *testing.T) {
	root := t.TempDir()
	writer, err := NewWithOptions(root, Options{
		ChunkMaxEvents:   1,
		ChunkMaxBytes:    1024,
		ChunkMaxDuration: time.Minute,
		JSONIgnoreError:  true,
		EnableLogfmt:     true,
	})
	if err != nil {
		t.Fatalf("new writer engine: %v", err)
	}
	defer writer.Close()

	reader, err := NewWithOptions(t.TempDir(), Options{
		ChunkMaxEvents:   1,
		ChunkMaxBytes:    1024,
		ChunkMaxDuration: time.Minute,
		JSONIgnoreError:  true,
		EnableLogfmt:     true,
		ObjectStore:      writer.objects,
	})
	if err != nil {
		t.Fatalf("new reader engine: %v", err)
	}
	defer reader.Close()

	reader.manifests = writer.manifests

	now := time.Now().UTC()
	empty, err := reader.Query(context.Background(), model.QueryRequest{
		TenantID:       "default",
		LabelSelectors: map[string]string{"service": "payments"},
		Start:          now.Add(-time.Minute),
		End:            now.Add(time.Minute),
		Limit:          10,
	})
	if err != nil {
		t.Fatalf("initial reader query: %v", err)
	}
	if empty.Matched != 0 {
		t.Fatalf("expected no matches before ingest, got %d", empty.Matched)
	}

	_, _, err = writer.Append(context.Background(), model.Event{
		Timestamp: now,
		TenantID:  "default",
		Body:      `{"service":"payments","status_code":500,"message":"timeout"}`,
	})
	if err != nil {
		t.Fatalf("append event: %v", err)
	}

	result, err := reader.Query(context.Background(), model.QueryRequest{
		TenantID:        "default",
		LabelSelectors:  map[string]string{"service": "payments"},
		FieldPredicates: map[string]string{"status_code": "500"},
		TextContains:    "timeout",
		Start:           now.Add(-time.Minute),
		End:             now.Add(time.Minute),
		Limit:           10,
	})
	if err != nil {
		t.Fatalf("reader query after ingest: %v", err)
	}
	if result.Matched != 1 {
		t.Fatalf("expected reader to observe new manifest without restart, got %d matches", result.Matched)
	}
}
