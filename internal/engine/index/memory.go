package index

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type MemoryIndex struct {
	mu          sync.RWMutex
	events      []model.Event
	labelIndex  map[string]map[string]map[int]struct{}
	fieldIndex  map[string]map[string]map[int]struct{}
	streamIndex map[string]map[int]struct{}
}

func NewMemoryIndex() *MemoryIndex {
	return &MemoryIndex{
		events:      []model.Event{},
		labelIndex:  map[string]map[string]map[int]struct{}{},
		fieldIndex:  map[string]map[string]map[int]struct{}{},
		streamIndex: map[string]map[int]struct{}{},
	}
}

func (m *MemoryIndex) Index(_ context.Context, event model.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	position := len(m.events)
	m.events = append(m.events, event.Clone())

	streamKey := event.StreamKey()
	if _, ok := m.streamIndex[streamKey]; !ok {
		m.streamIndex[streamKey] = map[int]struct{}{}
	}
	m.streamIndex[streamKey][position] = struct{}{}

	for key, value := range event.StreamLabels {
		addPosting(m.labelIndex, key, value, position)
	}
	for key, value := range event.ParsedFields {
		addPosting(m.fieldIndex, key, value, position)
	}
	return nil
}

func (m *MemoryIndex) Execute(_ context.Context, req model.QueryRequest) (model.QueryResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := req.Validate(); err != nil {
		return model.QueryResult{}, err
	}
	offset, err := req.CursorOffset()
	if err != nil {
		return model.QueryResult{}, err
	}
	matcher, err := compileRegex(req.TextRegex)
	if err != nil {
		return model.QueryResult{}, err
	}

	candidates := make(map[int]struct{}, len(m.events))
	candidateStreams := map[string]struct{}{}
	for idx, event := range m.events {
		if event.TenantID != req.TenantID {
			continue
		}
		if event.Timestamp.Before(req.Start) || event.Timestamp.After(req.End) {
			continue
		}
		candidates[idx] = struct{}{}
		candidateStreams[event.StreamKey()] = struct{}{}
	}

	for key, value := range req.LabelSelectors {
		candidates = intersect(candidates, m.labelIndex[key][value])
	}
	for key, value := range req.FieldPredicates {
		candidates = intersect(candidates, m.fieldIndex[key][value])
	}

	ids := make([]int, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	if req.Tail {
		reverseInts(ids)
	}

	result := model.QueryResult{
		Scanned: int64(len(ids)),
		Stats: model.Stats{
			CandidateStreams: len(candidateStreams),
			CandidateChunks:  len(candidateStreams),
			ScannedEvents:    len(ids),
		},
	}
	matches := make([]model.Event, 0, len(ids))
	for _, id := range ids {
		event := m.events[id]
		if req.TextContains != "" && !strings.Contains(strings.ToLower(event.Body), strings.ToLower(req.TextContains)) {
			continue
		}
		if matcher != nil && !matcher.MatchString(event.Body) {
			continue
		}
		matches = append(matches, event.Clone())
	}
	if offset > len(matches) {
		offset = len(matches)
	}
	remaining := matches[offset:]
	if req.Limit > 0 && len(remaining) > req.Limit {
		result.Events = append(result.Events, remaining[:req.Limit]...)
		next := offset + len(result.Events)
		if next < len(matches) {
			result.NextCursor = fmt.Sprintf("%d", next)
		}
	} else {
		result.Events = append(result.Events, remaining...)
	}
	result.Matched = int64(len(result.Events))
	return result, nil
}

func addPosting(target map[string]map[string]map[int]struct{}, key, value string, position int) {
	if _, ok := target[key]; !ok {
		target[key] = map[string]map[int]struct{}{}
	}
	if _, ok := target[key][value]; !ok {
		target[key][value] = map[int]struct{}{}
	}
	target[key][value][position] = struct{}{}
}

func intersect(left map[int]struct{}, right map[int]struct{}) map[int]struct{} {
	if len(left) == 0 || len(right) == 0 {
		return map[int]struct{}{}
	}

	out := make(map[int]struct{})
	for id := range left {
		if _, ok := right[id]; ok {
			out[id] = struct{}{}
		}
	}
	return out
}

func compileRegex(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, nil
	}
	if len(pattern) > 256 {
		return nil, fmt.Errorf("text_regex exceeds 256 characters")
	}
	return regexp.Compile(pattern)
}

func reverseInts(values []int) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}
