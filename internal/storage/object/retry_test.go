package object

import (
	"context"
	"errors"
	"testing"
	"time"
)

type flakyStore struct {
	failures int
}

func (f *flakyStore) Put(_ context.Context, key string, data []byte) (Metadata, error) {
	if f.failures > 0 {
		f.failures--
		return Metadata{}, errors.New("temporary write failure")
	}
	return Metadata{Key: key, Size: int64(len(data)), Checksum: checksumHex(data)}, nil
}

func (f *flakyStore) Get(context.Context, string) ([]byte, Metadata, error) {
	return nil, Metadata{}, nil
}

func (f *flakyStore) List(context.Context, string) ([]Metadata, error) {
	return nil, nil
}

func (f *flakyStore) Delete(context.Context, string) error {
	if f.failures > 0 {
		f.failures--
		return errors.New("temporary delete failure")
	}
	return nil
}

func TestRetryingStoreRetriesAndSucceeds(t *testing.T) {
	store := NewRetryingStore(&flakyStore{failures: 2}, 3, time.Millisecond)
	meta, err := store.Put(context.Background(), "chunks/a.zst", []byte("payload"))
	if err != nil {
		t.Fatalf("expected retry success, got %v", err)
	}
	if meta.Key != "chunks/a.zst" {
		t.Fatalf("unexpected metadata: %+v", meta)
	}
}

func TestRetryingStoreDeleteRetriesAndSucceeds(t *testing.T) {
	store := NewRetryingStore(&flakyStore{failures: 1}, 2, time.Millisecond)
	if err := store.Delete(context.Background(), "chunks/a.zst"); err != nil {
		t.Fatalf("expected delete retry success, got %v", err)
	}
}
