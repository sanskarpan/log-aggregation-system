package wal

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type SyncMode string

const (
	SyncAlways SyncMode = "always"
	SyncNever  SyncMode = "never"
)

type Options struct {
	MaxSegmentBytes int64
	SyncMode        SyncMode
}

type Writer struct {
	mu             sync.Mutex
	dir            string
	prefix         string
	options        Options
	currentFile    *os.File
	currentPath    string
	currentSegment int
	currentSize    int64
}

type record struct {
	Checksum string      `json:"checksum"`
	Event    model.Event `json:"event"`
}

func Open(path string) (*Writer, error) {
	return OpenWithOptions(path, Options{
		MaxSegmentBytes: 8 * 1024 * 1024,
		SyncMode:        SyncAlways,
	})
}

func OpenWithOptions(path string, options Options) (*Writer, error) {
	if options.MaxSegmentBytes <= 0 {
		options.MaxSegmentBytes = 8 * 1024 * 1024
	}
	if options.SyncMode == "" {
		options.SyncMode = SyncAlways
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	writer := &Writer{
		dir:     dir,
		prefix:  strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)),
		options: options,
	}
	if err := writer.openCurrentSegment(); err != nil {
		return nil, err
	}
	return writer, nil
}

func (w *Writer) Append(event model.Event) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry, err := marshalRecord(event)
	if err != nil {
		return err
	}
	if w.currentSize+int64(len(entry)) > w.options.MaxSegmentBytes && w.currentSize > 0 {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}
	if _, err := w.currentFile.Write(entry); err != nil {
		return err
	}
	w.currentSize += int64(len(entry))
	if w.options.SyncMode == SyncAlways {
		return w.currentFile.Sync()
	}
	return nil
}

func (w *Writer) Replay() ([]model.Event, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	paths, err := w.segmentPathsLocked()
	if err != nil {
		return nil, err
	}

	events := []model.Event{}
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			event, err := unmarshalRecord(scanner.Bytes())
			if err != nil {
				_ = file.Close()
				return nil, err
			}
			events = append(events, event)
		}
		if err := scanner.Err(); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := file.Close(); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (w *Writer) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.currentFile.Close(); err != nil {
		return err
	}
	paths, err := w.segmentPathsLocked()
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	w.currentSegment = 0
	w.currentSize = 0
	return w.openCurrentSegment()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.currentFile == nil {
		return nil
	}
	return w.currentFile.Close()
}

func (w *Writer) rotateLocked() error {
	if err := w.currentFile.Close(); err != nil {
		return err
	}
	w.currentSegment++
	w.currentSize = 0
	return w.openCurrentSegment()
}

func (w *Writer) openCurrentSegment() error {
	if w.currentSegment == 0 {
		paths, err := w.segmentPathsLocked()
		if err != nil {
			return err
		}
		if len(paths) > 0 {
			last := paths[len(paths)-1]
			fmt.Sscanf(filepath.Base(last), w.prefix+".%06d.wal", &w.currentSegment)
		}
	}

	path := filepath.Join(w.dir, fmt.Sprintf("%s.%06d.wal", w.prefix, w.currentSegment))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	w.currentFile = file
	w.currentPath = path
	w.currentSize = info.Size()
	return nil
}

func (w *Writer) segmentPathsLocked() ([]string, error) {
	pattern := filepath.Join(w.dir, w.prefix+".*.wal")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func marshalRecord(event model.Event) ([]byte, error) {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(eventPayload)
	entry, err := json.Marshal(record{
		Checksum: hex.EncodeToString(sum[:]),
		Event:    event,
	})
	if err != nil {
		return nil, err
	}
	return append(entry, '\n'), nil
}

func unmarshalRecord(line []byte) (model.Event, error) {
	var entry record
	if err := json.Unmarshal(line, &entry); err != nil {
		return model.Event{}, err
	}
	payload, err := json.Marshal(entry.Event)
	if err != nil {
		return model.Event{}, err
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != entry.Checksum {
		return model.Event{}, io.ErrUnexpectedEOF
	}
	return entry.Event, nil
}
