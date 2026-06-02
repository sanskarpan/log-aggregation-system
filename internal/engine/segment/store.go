package segment

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/engine/chunk"
)

type Segment struct {
	PartitionKey string         `json:"partition_key"`
	Start        time.Time      `json:"start"`
	End          time.Time      `json:"end"`
	Chunks       []*chunk.Chunk `json:"chunks"`
}

type Store struct {
	mu         sync.RWMutex
	bucketSize time.Duration
	segments   map[string]*Segment
}

func NewStore(bucketSize time.Duration) *Store {
	if bucketSize <= 0 {
		bucketSize = time.Hour
	}
	return &Store{
		bucketSize: bucketSize,
		segments:   map[string]*Segment{},
	}
}

func (s *Store) Add(ch *chunk.Chunk) *Segment {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := partitionKey(ch.Start, s.bucketSize)
	seg, ok := s.segments[key]
	if !ok {
		seg = &Segment{
			PartitionKey: key,
			Start:        bucketStart(ch.Start, s.bucketSize),
			End:          bucketStart(ch.Start, s.bucketSize).Add(s.bucketSize),
			Chunks:       []*chunk.Chunk{},
		}
		s.segments[key] = seg
	}
	seg.Chunks = append(seg.Chunks, ch)
	return seg
}

func (s *Store) List() []*Segment {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.segments))
	for key := range s.segments {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	out := make([]*Segment, 0, len(keys))
	for _, key := range keys {
		out = append(out, s.segments[key])
	}
	return out
}

func (s *Store) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.segments = map[string]*Segment{}
}

func partitionKey(ts time.Time, bucketSize time.Duration) string {
	start := bucketStart(ts, bucketSize)
	return fmt.Sprintf("%s/%s", start.UTC().Format("2006-01-02"), start.UTC().Format("15"))
}

func bucketStart(ts time.Time, bucketSize time.Duration) time.Time {
	unix := ts.UTC().Unix()
	size := int64(bucketSize.Seconds())
	return time.Unix((unix/size)*size, 0).UTC()
}
