package object

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	ErrKeyRequired    = errors.New("object key is required")
	ErrObjectConflict = errors.New("object already exists with different checksum")
)

type Metadata struct {
	Key       string    `json:"key"`
	Size      int64     `json:"size"`
	Checksum  string    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
}

type Store interface {
	Put(ctx context.Context, key string, data []byte) (Metadata, error)
	Get(ctx context.Context, key string) ([]byte, Metadata, error)
	List(ctx context.Context, prefix string) ([]Metadata, error)
	Delete(ctx context.Context, key string) error
}

type FileStore struct {
	root string
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root}
}

func (s *FileStore) Put(_ context.Context, key string, data []byte) (Metadata, error) {
	if strings.TrimSpace(key) == "" {
		return Metadata{}, ErrKeyRequired
	}

	path, err := s.path(key)
	if err != nil {
		return Metadata{}, err
	}
	checksum := checksumHex(data)

	if existing, err := os.ReadFile(path); err == nil {
		existingChecksum := checksumHex(existing)
		if existingChecksum != checksum {
			return Metadata{}, ErrObjectConflict
		}
		info, statErr := os.Stat(path)
		if statErr != nil {
			return Metadata{}, statErr
		}
		return Metadata{Key: key, Size: info.Size(), Checksum: checksum, CreatedAt: info.ModTime().UTC()}, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return Metadata{}, err
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return Metadata{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Metadata{}, err
	}
	return Metadata{Key: key, Size: info.Size(), Checksum: checksum, CreatedAt: info.ModTime().UTC()}, nil
}

func (s *FileStore) Get(_ context.Context, key string) ([]byte, Metadata, error) {
	if strings.TrimSpace(key) == "" {
		return nil, Metadata{}, ErrKeyRequired
	}

	path, err := s.path(key)
	if err != nil {
		return nil, Metadata{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, Metadata{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, Metadata{}, err
	}
	return data, Metadata{Key: key, Size: info.Size(), Checksum: checksumHex(data), CreatedAt: info.ModTime().UTC()}, nil
}

func (s *FileStore) List(_ context.Context, prefix string) ([]Metadata, error) {
	root := filepath.Clean(s.root)
	out := []Metadata{}
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return nil, err
	}

	if err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		out = append(out, Metadata{
			Key:       key,
			Size:      info.Size(),
			Checksum:  checksumHex(data),
			CreatedAt: info.ModTime().UTC(),
		})
		return nil
	}); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})
	return out, nil
}

func (s *FileStore) Delete(_ context.Context, key string) error {
	if strings.TrimSpace(key) == "" {
		return ErrKeyRequired
	}
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *FileStore) path(key string) (string, error) {
	cleaned := filepath.Clean(filepath.FromSlash(key))
	if cleaned == "." || strings.HasPrefix(cleaned, "..") || filepath.IsAbs(cleaned) {
		return "", ErrKeyRequired
	}
	return filepath.Join(s.root, cleaned), nil
}

func checksumHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
