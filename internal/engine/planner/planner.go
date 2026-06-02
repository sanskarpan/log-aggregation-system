package planner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/cache"
	"github.com/sanskar/log-aggregation-system/internal/engine/chunk"
	"github.com/sanskar/log-aggregation-system/internal/engine/segment"
	objectstore "github.com/sanskar/log-aggregation-system/internal/storage/object"
)

type Planner struct {
	segments   *segment.Store
	objects    objectstore.Store
	fallback   queryExecutor
	queryCache *cache.QueryCache
	chunkCache *cache.ChunkCache
	mu         sync.Mutex
}

type queryExecutor interface {
	Execute(context.Context, model.QueryRequest) (model.QueryResult, error)
}

type Fragment struct {
	PartitionKey string `json:"partition_key"`
	ChunkCount   int    `json:"chunk_count"`
}

func New(segments *segment.Store, objects objectstore.Store, fallback queryExecutor) *Planner {
	return NewWithOptions(segments, objects, fallback, Options{})
}

type Options struct {
	QueryCacheEntries int
	QueryCacheTTL     time.Duration
	ChunkCacheEntries int
}

func NewWithOptions(segments *segment.Store, objects objectstore.Store, fallback queryExecutor, opts Options) *Planner {
	return &Planner{
		segments:   segments,
		objects:    objects,
		fallback:   fallback,
		queryCache: cache.NewQueryCache(opts.QueryCacheEntries, opts.QueryCacheTTL),
		chunkCache: cache.NewChunkCache(opts.ChunkCacheEntries),
	}
}

func (p *Planner) Plan(ctx context.Context, req model.QueryRequest) (model.QueryPlan, error) {
	if err := req.Validate(); err != nil {
		return model.QueryPlan{}, err
	}
	fragments, candidates, err := p.candidateChunks(ctx, req)
	if err != nil {
		return model.QueryPlan{}, err
	}
	nodes := []model.PlanNode{
		{Type: "tenant", Description: fmt.Sprintf("tenant=%s", req.TenantID), Inputs: 1, Outputs: 1},
		{Type: "time", Description: fmt.Sprintf("range=%s..%s", req.Start.UTC().Format(time.RFC3339), req.End.UTC().Format(time.RFC3339)), Inputs: 1, Outputs: len(fragments)},
		{Type: "label-selector", Description: describeMap(req.LabelSelectors), Inputs: len(fragments), Outputs: len(candidates)},
	}
	if len(req.FieldPredicates) > 0 {
		nodes = append(nodes, model.PlanNode{Type: "field-selector", Description: describeMap(req.FieldPredicates), Inputs: len(candidates), Outputs: len(candidates)})
	}
	if req.TextContains != "" || req.TextRegex != "" {
		nodes = append(nodes, model.PlanNode{Type: "text-filter", Description: textDescription(req), Inputs: len(candidates), Outputs: len(candidates)})
	}
	candidateStreams := map[string]struct{}{}
	for _, c := range candidates {
		candidateStreams[c.StreamKey] = struct{}{}
	}
	return model.QueryPlan{
		Request:          req,
		Fragments:        fragmentsToPlanFragments(fragments),
		Nodes:            nodes,
		CandidateStreams: len(candidateStreams),
		CandidateChunks:  len(candidates),
		EstimatedCost:    len(candidates) * (1 + len(req.LabelSelectors) + len(req.FieldPredicates)),
	}, nil
}

func (p *Planner) Execute(ctx context.Context, req model.QueryRequest) (model.QueryResult, error) {
	if p.queryCache != nil {
		if cached, ok := p.queryCache.Get(requestCacheKey(req)); ok {
			return cached, nil
		}
	}
	plan, err := p.Plan(ctx, req)
	if err != nil {
		return model.QueryResult{}, err
	}
	fragments, candidates, err := p.candidateChunks(ctx, req)
	if err != nil {
		return model.QueryResult{}, err
	}
	_ = fragments

	matcher, err := compileRegex(req.TextRegex)
	if err != nil {
		return model.QueryResult{}, err
	}
	events := make([]model.Event, 0)
	scanned := 0
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		ch, err := p.loadChunk(ctx, candidate)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				result := model.QueryResult{Partial: true, Stats: model.Stats{CandidateStreams: plan.CandidateStreams, CandidateChunks: plan.CandidateChunks, ScannedEvents: scanned}}
				sortEvents(result.Events, req.Tail)
				return result, nil
			}
			return model.QueryResult{}, err
		}
		for _, event := range ch.Events {
			scanned++
			if !eventMatches(req, event, matcher) {
				continue
			}
			key := eventIdentity(event)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			events = append(events, event.Clone())
		}
	}
	if p.fallback != nil {
		hotReq := req
		hotReq.Limit = 0
		hotReq.Cursor = ""
		hot, err := p.fallback.Execute(ctx, hotReq)
		if err != nil {
			return model.QueryResult{}, err
		}
		scanned += int(hot.Scanned)
		for _, event := range hot.Events {
			key := eventIdentity(event)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			events = append(events, event.Clone())
		}
	}
	sortEvents(events, req.Tail)
	offset, err := req.CursorOffset()
	if err != nil {
		return model.QueryResult{}, err
	}
	result := model.QueryResult{
		Scanned: int64(scanned),
		Stats: model.Stats{
			CandidateStreams: plan.CandidateStreams,
			CandidateChunks:  plan.CandidateChunks,
			ScannedEvents:    scanned,
		},
	}
	if offset > len(events) {
		offset = len(events)
	}
	remaining := events[offset:]
	if req.Limit > 0 && len(remaining) > req.Limit {
		result.Events = append(result.Events, remaining[:req.Limit]...)
		next := offset + len(result.Events)
		if next < len(events) {
			result.NextCursor = fmt.Sprintf("%d", next)
		}
	} else {
		result.Events = append(result.Events, remaining...)
	}
	result.Matched = int64(len(result.Events))
	if p.queryCache != nil {
		p.queryCache.Put(requestCacheKey(req), result)
	}
	return result, nil
}

func (p *Planner) Explain(ctx context.Context, req model.QueryRequest) (model.QueryPlan, error) {
	return p.Plan(ctx, req)
}

func (p *Planner) CacheStats() map[string]any {
	stats := map[string]any{}
	if p.queryCache != nil {
		stats["query"] = p.queryCache.Stats()
	}
	if p.chunkCache != nil {
		stats["chunk"] = p.chunkCache.Stats()
	}
	return stats
}

func (p *Planner) InvalidateAll() {
	if p.queryCache != nil {
		p.queryCache.Reset()
	}
	if p.chunkCache != nil {
		p.chunkCache.Reset()
	}
}

func (p *Planner) WarmRecentChunks(ctx context.Context, limit int) error {
	if p == nil || p.chunkCache == nil || p.objects == nil || p.segments == nil {
		return nil
	}
	if limit <= 0 {
		limit = 32
	}
	segments := p.segments.List()
	warmed := 0
	for i := len(segments) - 1; i >= 0 && warmed < limit; i-- {
		seg := segments[i]
		for j := len(seg.Chunks) - 1; j >= 0 && warmed < limit; j-- {
			ch := seg.Chunks[j]
			if ch == nil || ch.ObjectKey == "" {
				continue
			}
			if cached, ok := p.chunkCache.Get(ch.ObjectKey); ok && cached != nil {
				warmed++
				continue
			}
			data, _, err := p.objects.Get(ctx, ch.ObjectKey)
			if err != nil {
				return err
			}
			decoded, err := chunk.Decode(data)
			if err != nil {
				return err
			}
			p.chunkCache.Put(ch.ObjectKey, &decoded)
			warmed++
		}
	}
	return nil
}

type candidate struct {
	StreamKey string
	Chunk     *chunk.Chunk
	Segment   string
}

func (p *Planner) candidateChunks(ctx context.Context, req model.QueryRequest) ([]Fragment, []candidate, error) {
	segs := p.segments.List()
	fragments := make([]Fragment, 0, len(segs))
	candidates := make([]candidate, 0)
	for _, seg := range segs {
		if req.PartitionKey != "" && seg.PartitionKey != req.PartitionKey {
			continue
		}
		if seg.End.Before(req.Start) || seg.Start.After(req.End) {
			continue
		}
		frag := Fragment{PartitionKey: seg.PartitionKey}
		for _, ch := range seg.Chunks {
			if ch == nil {
				continue
			}
			if ch.End.Before(req.Start) || ch.Start.After(req.End) {
				continue
			}
			if !chunkMatchesLabels(ch, req.LabelSelectors) {
				continue
			}
			if !chunkMatchesFields(ch, req.FieldPredicates) {
				continue
			}
			if !chunkMatchesTextSummary(ch, req.TextContains, req.TextRegex) {
				continue
			}
			candidates = append(candidates, candidate{
				StreamKey: ch.StreamKey,
				Chunk:     ch,
				Segment:   seg.PartitionKey,
			})
			frag.ChunkCount++
		}
		if frag.ChunkCount > 0 {
			fragments = append(fragments, frag)
		}
	}
	return fragments, candidates, nil
}

func (p *Planner) loadChunk(ctx context.Context, candidate candidate) (*chunk.Chunk, error) {
	if candidate.Chunk == nil {
		return nil, errors.New("candidate chunk missing")
	}
	if len(candidate.Chunk.Events) > 0 {
		if p.chunkCache != nil && candidate.Chunk.ObjectKey != "" {
			p.chunkCache.Put(candidate.Chunk.ObjectKey, candidate.Chunk)
		}
		return candidate.Chunk, nil
	}
	if p.chunkCache != nil && candidate.Chunk.ObjectKey != "" {
		if cached, ok := p.chunkCache.Get(candidate.Chunk.ObjectKey); ok {
			return cached, nil
		}
	}
	if p.objects == nil || candidate.Chunk.ObjectKey == "" {
		return candidate.Chunk, nil
	}
	data, _, err := p.objects.Get(ctx, candidate.Chunk.ObjectKey)
	if err != nil {
		return nil, err
	}
	decoded, err := chunk.Decode(data)
	if err != nil {
		return nil, err
	}
	if p.chunkCache != nil {
		p.chunkCache.Put(candidate.Chunk.ObjectKey, &decoded)
	}
	return &decoded, nil
}

func eventMatches(req model.QueryRequest, event model.Event, matcher *regexp.Regexp) bool {
	if event.TenantID != req.TenantID {
		return false
	}
	if event.Timestamp.Before(req.Start) || event.Timestamp.After(req.End) {
		return false
	}
	for key, value := range req.LabelSelectors {
		if event.StreamLabels[key] != value {
			return false
		}
	}
	for key, value := range req.FieldPredicates {
		if event.ParsedFields[key] != value && event.LogAttrs[key] != value && event.ResourceAttrs[key] != value {
			return false
		}
	}
	if req.TextContains != "" && !strings.Contains(strings.ToLower(event.Body), strings.ToLower(req.TextContains)) {
		return false
	}
	if matcher != nil && !matcher.MatchString(event.Body) {
		return false
	}
	return true
}

func chunkMatchesLabels(ch *chunk.Chunk, selectors map[string]string) bool {
	if len(selectors) == 0 {
		return true
	}
	for key, value := range selectors {
		if ch.Metadata.StreamLabels[key] != value {
			return false
		}
	}
	return true
}

func chunkMatchesFields(ch *chunk.Chunk, predicates map[string]string) bool {
	if len(predicates) == 0 {
		return true
	}
	for key := range predicates {
		found := false
		for _, candidate := range ch.Metadata.SearchableFields {
			if candidate == key {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func chunkMatchesTextSummary(ch *chunk.Chunk, contains, regex string) bool {
	if contains == "" && regex == "" {
		return true
	}
	if regex != "" {
		return true
	}
	tokens := strings.FieldsFunc(strings.ToLower(contains), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	if len(tokens) == 0 {
		return true
	}
	for _, token := range tokens {
		if !chunk.BloomContains(ch.Metadata.BodyBloom, token) {
			return false
		}
	}
	return true
}

func sortEvents(events []model.Event, tail bool) {
	sort.SliceStable(events, func(i, j int) bool {
		if tail {
			return events[i].Timestamp.After(events[j].Timestamp)
		}
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
}

func describeMap(input map[string]string) string {
	if len(input) == 0 {
		return "none"
	}
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, input[key]))
	}
	return strings.Join(parts, ",")
}

func textDescription(req model.QueryRequest) string {
	if req.TextRegex != "" {
		return "regex=" + req.TextRegex
	}
	return "contains=" + req.TextContains
}

func eventIdentity(event model.Event) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s", event.TenantID, event.Timestamp.UTC().Format(time.RFC3339Nano), event.StreamKey(), event.Body, event.TraceID, event.SpanID)
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

func requestCacheKey(req model.QueryRequest) string {
	payload, _ := json.Marshal(req)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func fragmentsToPlanFragments(fragments []Fragment) []model.QueryFragment {
	if len(fragments) == 0 {
		return nil
	}
	out := make([]model.QueryFragment, 0, len(fragments))
	for _, fragment := range fragments {
		out = append(out, model.QueryFragment{
			PartitionKey: fragment.PartitionKey,
			ChunkCount:   fragment.ChunkCount,
		})
	}
	return out
}
