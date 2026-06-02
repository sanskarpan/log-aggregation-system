package singlenode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/chunk"
	"github.com/sanskar/log-aggregation-system/internal/engine/index"
	"github.com/sanskar/log-aggregation-system/internal/engine/pipeline"
	"github.com/sanskar/log-aggregation-system/internal/engine/planner"
	"github.com/sanskar/log-aggregation-system/internal/engine/quota"
	"github.com/sanskar/log-aggregation-system/internal/engine/segment"
	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
	"github.com/sanskar/log-aggregation-system/internal/engine/wal"
	manifests "github.com/sanskar/log-aggregation-system/internal/storage/manifests"
	objectstore "github.com/sanskar/log-aggregation-system/internal/storage/object"
)

type Engine struct {
	tenants          tenant.Repository
	parser           *pipeline.Processor
	index            *index.MemoryIndex
	segments         *segment.Store
	wal              *wal.Writer
	builders         map[string]*chunk.Builder
	pipelines        map[string]model.ParsePipeline
	objects          objectstore.Store
	manifests        manifests.Repository
	plan             *planner.Planner
	queryLimiter     *quota.ConcurrencyLimiter
	options          Options
	replayed         bool
	manifestRevision string
	refreshMu        sync.Mutex
}

type Options struct {
	ChunkMaxEvents     int
	ChunkMaxBytes      int
	ChunkMaxDuration   time.Duration
	QueryCacheEntries  int
	QueryCacheTTL      time.Duration
	ChunkCacheEntries  int
	TenantDSN          string
	ManifestDSN        string
	JSONIgnoreError    bool
	EnableLogfmt       bool
	WALMaxSegmentBytes int64
	WALSyncMode        wal.SyncMode
	SegmentBucket      time.Duration
	ObjectStore        objectstore.Store
	ObjectStoreRetries int
	ObjectStoreBackoff time.Duration
}

func New(root string) (*Engine, error) {
	return NewWithOptions(root, Options{
		ChunkMaxEvents:     1000,
		ChunkMaxBytes:      256 * 1024,
		ChunkMaxDuration:   time.Minute,
		QueryCacheEntries:  256,
		QueryCacheTTL:      30 * time.Second,
		ChunkCacheEntries:  512,
		JSONIgnoreError:    true,
		EnableLogfmt:       true,
		WALMaxSegmentBytes: 8 * 1024 * 1024,
		WALSyncMode:        wal.SyncAlways,
		SegmentBucket:      time.Hour,
	})
}

func NewWithOptions(root string, options Options) (*Engine, error) {
	writer, err := wal.OpenWithOptions(filepath.Join(root, "wal", "events.log"), wal.Options{
		MaxSegmentBytes: options.WALMaxSegmentBytes,
		SyncMode:        options.WALSyncMode,
	})
	if err != nil {
		return nil, err
	}

	if options.ChunkMaxEvents <= 0 {
		options.ChunkMaxEvents = 1000
	}
	if options.ChunkMaxBytes <= 0 {
		options.ChunkMaxBytes = 256 * 1024
	}
	if options.ChunkMaxDuration <= 0 {
		options.ChunkMaxDuration = time.Minute
	}
	if options.QueryCacheEntries <= 0 {
		options.QueryCacheEntries = 256
	}
	if options.QueryCacheTTL <= 0 {
		options.QueryCacheTTL = 30 * time.Second
	}
	if options.ChunkCacheEntries <= 0 {
		options.ChunkCacheEntries = 512
	}
	if options.SegmentBucket <= 0 {
		options.SegmentBucket = time.Hour
	}
	if options.ObjectStoreRetries < 0 {
		options.ObjectStoreRetries = 0
	}
	if options.ObjectStoreBackoff <= 0 {
		options.ObjectStoreBackoff = 50 * time.Millisecond
	}
	if options.ObjectStore == nil {
		options.ObjectStore = objectstore.NewRetryingStore(
			objectstore.NewFileStore(filepath.Join(root, "objects")),
			options.ObjectStoreRetries,
			options.ObjectStoreBackoff,
		)
	}

	stages := []model.PipelineStage{
		{Name: "json", Type: "json", Config: map[string]string{"ignore_error": boolString(options.JSONIgnoreError)}},
	}
	if options.EnableLogfmt {
		stages = append(stages, model.PipelineStage{Name: "logfmt", Type: "logfmt"})
	}
	stages = append(stages,
		model.PipelineStage{Name: "timestamp", Type: "timestamp", Config: map[string]string{"source": "timestamp", "format": "rfc3339", "ignore_error": "true"}},
		model.PipelineStage{Name: "severity", Type: "severity", Config: map[string]string{"source": "level", "ignore_error": "true"}},
	)

	var tenantStore tenant.Repository
	if options.TenantDSN != "" {
		tenantStore, err = tenant.NewPostgresStore(context.Background(), options.TenantDSN)
		if err != nil {
			return nil, err
		}
	} else {
		tenantStore, err = tenant.NewPersistentStore(filepath.Join(root, "control", "tenants.json"))
		if err != nil {
			return nil, err
		}
	}
	var manifestRepo manifests.Repository
	if options.ManifestDSN != "" {
		manifestRepo, err = manifests.NewPostgresRepository(context.Background(), options.ManifestDSN)
		if err != nil {
			return nil, err
		}
	} else {
		manifestRepo = manifests.NewMemoryRepository()
	}

	segments := segment.NewStore(options.SegmentBucket)
	idx := index.NewMemoryIndex()
	eng := &Engine{
		tenants:      tenantStore,
		parser:       pipeline.NewProcessor(),
		index:        idx,
		segments:     segments,
		wal:          writer,
		builders:     map[string]*chunk.Builder{},
		objects:      options.ObjectStore,
		manifests:    manifestRepo,
		queryLimiter: quota.NewConcurrencyLimiter(),
		options:      options,
		pipelines: map[string]model.ParsePipeline{
			"default": {
				ID:          "default",
				Description: "Parse JSON and logfmt payloads where possible.",
				Stages:      stages,
			},
		},
	}
	eng.plan = planner.NewWithOptions(segments, options.ObjectStore, idx, planner.Options{
		QueryCacheEntries: options.QueryCacheEntries,
		QueryCacheTTL:     options.QueryCacheTTL,
		ChunkCacheEntries: options.ChunkCacheEntries,
	})
	return eng, nil
}

func (e *Engine) Close() error {
	err := e.wal.Close()
	if closer, ok := e.manifests.(interface{ Close() error }); ok {
		if closeErr := closer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	if closer, ok := e.tenants.(interface{ Close() error }); ok {
		if closeErr := closer.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func (e *Engine) Append(ctx context.Context, event model.Event) (model.Event, *chunk.Chunk, error) {
	tenantConfig, err := e.tenants.GetTenant(ctx, event.TenantID)
	if err != nil {
		return model.Event{}, nil, err
	}

	pipelineConfig := e.pipelineForTenant(tenantConfig)
	processed, err := e.parser.Process(ctx, tenantConfig, event, pipelineConfig)
	if err != nil {
		return model.Event{}, nil, err
	}
	if processed.LogAttrs["_pipeline_drop"] == "true" {
		return model.Event{}, nil, nil
	}
	if err := processed.ValidateWithLimits(tenantConfig.Limits); err != nil {
		return model.Event{}, nil, err
	}
	if err := e.wal.Append(processed); err != nil {
		return model.Event{}, nil, err
	}
	if err := e.index.Index(ctx, processed); err != nil {
		return model.Event{}, nil, err
	}

	streamKey := processed.StreamKey()
	builder, ok := e.builders[streamKey]
	if !ok {
		builder = chunk.NewBuilderWithOptions(streamKey, chunk.BuilderOptions{
			MaxEvents:   e.options.ChunkMaxEvents,
			MaxBytes:    e.options.ChunkMaxBytes,
			MaxDuration: e.options.ChunkMaxDuration,
		})
		e.builders[streamKey] = builder
	}
	_, flushed := builder.Add(processed)
	if flushed != nil {
		if err := e.persistFlushedChunk(ctx, flushed); err != nil {
			return model.Event{}, nil, err
		}
		seg := e.segments.Add(flushed)
		if err := e.persistSegmentManifest(ctx, seg); err != nil {
			return model.Event{}, nil, err
		}
		if e.plan != nil {
			e.plan.InvalidateAll()
		}
	}
	return processed, flushed, nil
}

func (e *Engine) Query(ctx context.Context, req model.QueryRequest) (model.QueryResult, error) {
	tenantConfig, err := e.tenants.GetTenant(ctx, req.TenantID)
	if err != nil {
		return model.QueryResult{}, err
	}
	release, err := e.queryLimiter.TryAcquire(tenantConfig.ID, tenantConfig.Limits.QueryConcurrency)
	if err != nil {
		return model.QueryResult{}, err
	}
	defer release()
	return e.executeQuery(ctx, req)
}

func (e *Engine) Tail(ctx context.Context, req model.TailRequest) (model.QueryResult, error) {
	return e.Query(ctx, model.QueryRequest{
		TenantID:        req.TenantID,
		LabelSelectors:  req.LabelSelectors,
		FieldPredicates: req.FieldPredicates,
		Start:           firstNonZeroTime(req.Since, time.Now().UTC().Add(-5*time.Minute)),
		End:             time.Now().UTC(),
		Limit:           req.Limit,
		Tail:            true,
	})
}

func (e *Engine) Explain(ctx context.Context, req model.QueryRequest) (model.QueryPlan, error) {
	tenantConfig, err := e.tenants.GetTenant(ctx, req.TenantID)
	if err != nil {
		return model.QueryPlan{}, err
	}
	release, err := e.queryLimiter.TryAcquire(tenantConfig.ID, tenantConfig.Limits.QueryConcurrency)
	if err != nil {
		return model.QueryPlan{}, err
	}
	defer release()
	if err := e.refreshSegments(ctx); err != nil {
		return model.QueryPlan{}, err
	}
	if e.plan != nil {
		return e.plan.Explain(ctx, req)
	}
	return model.QueryPlan{}, nil
}

func (e *Engine) Replay(ctx context.Context) ([]model.Event, error) {
	if e.replayed {
		return nil, nil
	}
	events, err := e.wal.Replay()
	if err != nil {
		return nil, err
	}

	for _, event := range events {
		if err := e.index.Index(ctx, event); err != nil {
			return nil, err
		}
	}
	if err := e.restoreSegments(ctx); err != nil {
		return nil, err
	}
	e.manifestRevision = ""
	if e.plan != nil {
		if err := e.plan.WarmRecentChunks(ctx, 32); err != nil {
			return nil, err
		}
	}
	e.replayed = true
	return events, nil
}

func (e *Engine) ListTenants(ctx context.Context) []model.TenantConfig {
	return e.tenants.List(ctx)
}

func (e *Engine) Segments() []*segment.Segment {
	return e.segments.List()
}

func (e *Engine) ParserStats() model.ParseStats {
	return e.parser.Stats()
}

func (e *Engine) ObjectStats(ctx context.Context) ([]objectstore.Metadata, error) {
	if e.objects == nil {
		return nil, nil
	}
	return e.objects.List(ctx, "")
}

func (e *Engine) CacheStats() map[string]any {
	if e.plan == nil {
		return map[string]any{}
	}
	return e.plan.CacheStats()
}

func (e *Engine) pipelineForTenant(tenantConfig model.TenantConfig) model.ParsePipeline {
	if len(tenantConfig.Pipelines) > 0 {
		if tenantConfig.ActivePipelineID != "" {
			for _, pipelineConfig := range tenantConfig.Pipelines {
				if pipelineConfig.ID == tenantConfig.ActivePipelineID {
					return pipelineConfig
				}
			}
		}
		return tenantConfig.Pipelines[0]
	}
	if pipelineConfig, ok := e.pipelines[tenantConfig.ID]; ok {
		return pipelineConfig
	}
	return e.pipelines["default"]
}

func boolString(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func firstNonZeroTime(value, fallback time.Time) time.Time {
	if value.IsZero() {
		return fallback
	}
	return value
}

type segmentManifest struct {
	PartitionKey string           `json:"partition_key"`
	Start        time.Time        `json:"start"`
	End          time.Time        `json:"end"`
	Chunks       []chunkReference `json:"chunks"`
	GeneratedAt  time.Time        `json:"generated_at"`
}

type chunkReference struct {
	StreamKey string         `json:"stream_key"`
	ObjectKey string         `json:"object_key"`
	Checksum  string         `json:"checksum"`
	Start     time.Time      `json:"start"`
	End       time.Time      `json:"end"`
	Metadata  chunk.Metadata `json:"metadata"`
}

func (e *Engine) persistFlushedChunk(ctx context.Context, flushed *chunk.Chunk) error {
	if e.objects == nil || flushed == nil {
		return nil
	}
	encoded, err := chunk.Encode(*flushed)
	if err != nil {
		return err
	}
	key := chunkObjectKey(flushed, encoded)
	meta, err := e.objects.Put(ctx, key, encoded)
	if err != nil {
		return err
	}
	flushed.ObjectKey = meta.Key
	flushed.Checksum = meta.Checksum
	return nil
}

func (e *Engine) persistSegmentManifest(ctx context.Context, seg *segment.Segment) error {
	if e.objects == nil || seg == nil {
		return nil
	}
	manifest := segmentManifest{
		PartitionKey: seg.PartitionKey,
		Start:        seg.Start,
		End:          seg.End,
		Chunks:       make([]chunkReference, 0, len(seg.Chunks)),
		GeneratedAt:  time.Now().UTC(),
	}
	for _, ch := range seg.Chunks {
		manifest.Chunks = append(manifest.Chunks, chunkReference{
			StreamKey: ch.StreamKey,
			ObjectKey: ch.ObjectKey,
			Checksum:  ch.Checksum,
			Start:     ch.Start,
			End:       ch.End,
			Metadata:  ch.Metadata,
		})
	}
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	key := segmentManifestKey(seg.PartitionKey, data)
	_, err = e.objects.Put(ctx, key, data)
	if err != nil {
		return err
	}
	if e.manifests != nil {
		_, err = e.manifests.Put(ctx, manifests.Record{
			Key:          key,
			PartitionKey: seg.PartitionKey,
			Start:        seg.Start,
			End:          seg.End,
			Checksum:     manifests.Checksum(data),
			Payload:      data,
			CreatedAt:    time.Now().UTC(),
		})
	}
	return err
}

func chunkObjectKey(ch *chunk.Chunk, payload []byte) string {
	hash := sha256.Sum256(payload)
	return "chunks/" +
		ch.Start.UTC().Format("2006/01/02/15") +
		"/" + hex.EncodeToString(hash[:8]) +
		"/" + ch.Start.UTC().Format("20060102T150405.000000000Z") +
		"-" + ch.End.UTC().Format("20060102T150405.000000000Z") +
		".zst"
}

type persistedSegmentManifest struct {
	PartitionKey string              `json:"partition_key"`
	Start        time.Time           `json:"start"`
	End          time.Time           `json:"end"`
	Chunks       []persistedChunkRef `json:"chunks"`
	GeneratedAt  time.Time           `json:"generated_at"`
}

func segmentManifestKey(partitionKey string, payload []byte) string {
	hash := sha256.Sum256(payload)
	return "segments/" + partitionKey + "/manifests/" + time.Now().UTC().Format("20060102T150405.000000000Z") + "-" + hex.EncodeToString(hash[:8]) + ".json"
}

type persistedChunkRef struct {
	StreamKey string         `json:"stream_key"`
	ObjectKey string         `json:"object_key"`
	Checksum  string         `json:"checksum"`
	Start     time.Time      `json:"start"`
	End       time.Time      `json:"end"`
	Metadata  chunk.Metadata `json:"metadata"`
}

func (e *Engine) restoreSegments(ctx context.Context) error {
	if e.objects == nil {
		return nil
	}
	records := make([]manifests.Record, 0)
	if e.manifests != nil {
		repoRecords, err := e.manifests.List(ctx, "segments/")
		if err != nil {
			return err
		}
		records = append(records, repoRecords...)
	}
	if len(records) == 0 {
		metas, err := e.objects.List(ctx, "segments/")
		if err != nil {
			return err
		}
		for _, meta := range metas {
			data, _, err := e.objects.Get(ctx, meta.Key)
			if err != nil {
				return err
			}
			records = append(records, manifests.Record{Key: meta.Key, Payload: data})
		}
	}
	for _, record := range records {
		data := record.Payload
		if len(data) == 0 {
			fetched, _, err := e.objects.Get(ctx, record.Key)
			if err != nil {
				return err
			}
			data = fetched
		}
		var manifest persistedSegmentManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return err
		}
		for _, ref := range manifest.Chunks {
			ch := &chunk.Chunk{
				StreamKey: ref.StreamKey,
				Start:     ref.Start,
				End:       ref.End,
				ObjectKey: ref.ObjectKey,
				Checksum:  ref.Checksum,
				Metadata:  ref.Metadata,
			}
			e.segments.Add(ch)
		}
	}
	return nil
}

func (e *Engine) refreshSegments(ctx context.Context) error {
	if e == nil || e.objects == nil || e.manifests == nil || e.segments == nil {
		return nil
	}

	records, err := e.manifests.List(ctx, "segments/")
	if err != nil {
		return err
	}
	revision := manifestRevision(records)

	e.refreshMu.Lock()
	defer e.refreshMu.Unlock()
	if revision == e.manifestRevision {
		return nil
	}

	e.segments.Clear()
	if err := e.loadSegmentsFromRecords(ctx, records); err != nil {
		return err
	}
	e.manifestRevision = revision
	if e.plan != nil {
		e.plan.InvalidateAll()
	}
	return nil
}

func (e *Engine) executeQuery(ctx context.Context, req model.QueryRequest) (model.QueryResult, error) {
	if err := e.refreshSegments(ctx); err != nil {
		return model.QueryResult{}, err
	}
	if e.plan != nil {
		return e.plan.Execute(ctx, req)
	}
	return e.index.Execute(ctx, req)
}

func (e *Engine) loadSegmentsFromRecords(ctx context.Context, records []manifests.Record) error {
	if e.objects == nil {
		return nil
	}
	if len(records) == 0 {
		metas, err := e.objects.List(ctx, "segments/")
		if err != nil {
			return err
		}
		for _, meta := range metas {
			data, _, err := e.objects.Get(ctx, meta.Key)
			if err != nil {
				return err
			}
			records = append(records, manifests.Record{Key: meta.Key, Payload: data})
		}
	}
	for _, record := range records {
		data := record.Payload
		if len(data) == 0 {
			fetched, _, err := e.objects.Get(ctx, record.Key)
			if err != nil {
				return err
			}
			data = fetched
		}
		var manifest persistedSegmentManifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			return err
		}
		for _, ref := range manifest.Chunks {
			ch := &chunk.Chunk{
				StreamKey: ref.StreamKey,
				Start:     ref.Start,
				End:       ref.End,
				ObjectKey: ref.ObjectKey,
				Checksum:  ref.Checksum,
				Metadata:  ref.Metadata,
			}
			e.segments.Add(ch)
		}
	}
	return nil
}

func manifestRevision(records []manifests.Record) string {
	if len(records) == 0 {
		return ""
	}
	keys := make([]string, 0, len(records))
	for _, record := range records {
		keys = append(keys, record.Key+"|"+record.Checksum+"|"+record.CreatedAt.UTC().Format(time.RFC3339Nano))
	}
	sort.Strings(keys)
	return strings.Join(keys, "\n")
}
