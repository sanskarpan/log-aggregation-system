package model

import (
	"testing"
	"time"
)

func TestQueryRequestValidate(t *testing.T) {
	req := QueryRequest{
		TenantID: "tenant-a",
		Start:    time.Now().Add(-time.Hour),
		End:      time.Now(),
	}

	if err := req.Validate(); err != nil {
		t.Fatalf("expected valid query request, got %v", err)
	}
}

func TestQueryRequestCursorOffset(t *testing.T) {
	req := QueryRequest{Cursor: "12"}
	offset, err := req.CursorOffset()
	if err != nil {
		t.Fatalf("unexpected cursor error: %v", err)
	}
	if offset != 12 {
		t.Fatalf("unexpected offset: %d", offset)
	}
}
