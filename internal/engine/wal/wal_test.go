package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestWriterAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal", "events.log")
	writer, err := OpenWithOptions(path, Options{
		MaxSegmentBytes: 128,
		SyncMode:        SyncAlways,
	})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer writer.Close()

	event := model.Event{
		Timestamp:    time.Now().UTC(),
		TenantID:     "tenant-a",
		StreamLabels: map[string]string{"service": "api"},
		Body:         "hello",
	}
	for i := 0; i < 5; i++ {
		if err := writer.Append(event); err != nil {
			t.Fatalf("append wal: %v", err)
		}
	}

	events, err := writer.Replay()
	if err != nil {
		t.Fatalf("replay wal: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}

	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "events.*.wal"))
	if err != nil {
		t.Fatalf("glob wal segments: %v", err)
	}
	if len(files) < 2 {
		t.Fatalf("expected wal rotation, got %d segments", len(files))
	}
}

func TestWriterChecksumFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal", "events.log")
	writer, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	event := model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "tenant-a",
		Body:      "hello",
	}
	if err := writer.Append(event); err != nil {
		t.Fatalf("append wal: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(filepath.Dir(path), "events.*.wal"))
	if err != nil || len(files) == 0 {
		t.Fatalf("glob wal files: %v", err)
	}
	if err := os.WriteFile(files[0], []byte("corrupt\n"), 0o644); err != nil {
		t.Fatalf("corrupt wal file: %v", err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Replay(); err == nil {
		t.Fatal("expected checksum failure")
	}
}
