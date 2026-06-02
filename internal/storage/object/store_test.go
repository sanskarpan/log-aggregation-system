package object

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestFileStorePutGetListAndIdempotency(t *testing.T) {
	ctx := context.Background()
	store := NewFileStore(t.TempDir())

	meta, err := store.Put(ctx, "chunks/a.zst", []byte("payload"))
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	if meta.Key != "chunks/a.zst" || meta.Size != int64(len("payload")) || meta.Checksum == "" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}

	same, err := store.Put(ctx, "chunks/a.zst", []byte("payload"))
	if err != nil {
		t.Fatalf("idempotent put object: %v", err)
	}
	if same.Checksum != meta.Checksum {
		t.Fatalf("expected same checksum, got %s and %s", same.Checksum, meta.Checksum)
	}

	if _, err := store.Put(ctx, "chunks/a.zst", []byte("different")); !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	data, fetched, err := store.Get(ctx, "chunks/a.zst")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	if !bytes.Equal(data, []byte("payload")) || fetched.Checksum != meta.Checksum {
		t.Fatalf("unexpected fetched object: %+v", fetched)
	}

	list, err := store.List(ctx, "chunks/")
	if err != nil {
		t.Fatalf("list objects: %v", err)
	}
	if len(list) != 1 || list[0].Key != "chunks/a.zst" {
		t.Fatalf("unexpected object list: %+v", list)
	}

	if err := store.Delete(ctx, "chunks/a.zst"); err != nil {
		t.Fatalf("delete object: %v", err)
	}
	list, err = store.List(ctx, "chunks/")
	if err != nil {
		t.Fatalf("list objects after delete: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected empty list after delete, got %+v", list)
	}
}
