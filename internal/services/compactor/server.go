package compactor

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
	"github.com/sanskar/log-aggregation-system/internal/platform/config"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
	manifests "github.com/sanskar/log-aggregation-system/internal/storage/manifests"
	objectstore "github.com/sanskar/log-aggregation-system/internal/storage/object"
)

type Server struct {
	desc      servicehttp.Descriptor
	tenants   tenant.Repository
	manifests manifests.Repository
	objects   objectstore.Store
	dataDir   string
}

type CompactRequest struct {
	TenantID      string `json:"tenant_id,omitempty"`
	DryRun        bool   `json:"dry_run,omitempty"`
	PartitionKey  string `json:"partition_key,omitempty"`
	DeleteOnlyOld bool   `json:"delete_only_old,omitempty"`
}

type CompactResponse struct {
	ScannedManifests int                       `json:"scanned_manifests"`
	DeletedManifests int                       `json:"deleted_manifests"`
	DeletedChunks    int                       `json:"deleted_chunks"`
	DeleteManifests  int                       `json:"delete_manifests"`
	DryRun           bool                      `json:"dry_run"`
	ByTenant         []TenantCompactionSummary `json:"by_tenant"`
}

type TenantCompactionSummary struct {
	TenantID        string `json:"tenant_id"`
	RetentionClass  string `json:"retention_class"`
	RetentionPeriod string `json:"retention_period"`
	Scanned         int    `json:"scanned"`
	Deleted         int    `json:"deleted"`
}

func NewServer(cfg config.Config) (*Server, error) {
	tenants, err := buildTenantRepo(cfg)
	if err != nil {
		return nil, err
	}
	manifestRepo, err := buildManifestRepo(cfg)
	if err != nil {
		return nil, err
	}
	objects := objectstore.NewRetryingStore(
		objectstore.NewFileStore(filepath.Join(cfg.DataDir, "objects")),
		cfg.ObjectStoreRetries,
		cfg.ObjectStoreBackoff,
	)
	return &Server{
		desc:      Descriptor(),
		tenants:   tenants,
		manifests: manifestRepo,
		objects:   objects,
		dataDir:   cfg.DataDir,
	}, nil
}

func NewServerWithDeps(tenants tenant.Repository, manifestRepo manifests.Repository, objects objectstore.Store) *Server {
	if tenants == nil {
		tenants = tenant.NewStoreWithDefaults()
	}
	if manifestRepo == nil {
		manifestRepo = manifests.NewMemoryRepository()
	}
	if objects == nil {
		objects = objectstore.NewFileStore(".data/objects")
	}
	return &Server{
		desc:      Descriptor(),
		tenants:   tenants,
		manifests: manifestRepo,
		objects:   objects,
		dataDir:   ".data",
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	servicehttp.RegisterBaseRoutes(mux, s.desc)
	mux.HandleFunc("/internal/v1/compact", s.handleCompact)
	mux.HandleFunc("/internal/v1/status", s.handleStatus)
	return mux
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"data_dir": s.dataDir,
		"ready":    true,
	})
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req CompactRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	resp, err := s.Compact(r.Context(), req)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, resp)
}

func (s *Server) Compact(ctx context.Context, req CompactRequest) (CompactResponse, error) {
	tenants := s.tenants.List(ctx)
	tenantMap := map[string]model.TenantConfig{}
	for _, tenantCfg := range tenants {
		tenantMap[tenantCfg.ID] = tenantCfg
	}

	records, err := s.manifestRecords(ctx, req.PartitionKey)
	if err != nil {
		return CompactResponse{}, err
	}

	summaries := map[string]*TenantCompactionSummary{}
	resp := CompactResponse{DryRun: req.DryRun}
	for _, record := range records {
		manifest, err := decodeManifest(ctx, record.Payload, s.objects, record.Key)
		if err != nil {
			return CompactResponse{}, err
		}
		tenantID := tenantIDFromManifest(manifest)
		if req.TenantID != "" && tenantID != req.TenantID {
			continue
		}
		tenantCfg, ok := tenantMap[tenantID]
		if !ok {
			tenantCfg = tenant.DefaultTenantConfig()
			tenantCfg.ID = tenantID
		}
		if tenantCfg.Protected || (!tenantCfg.LegalHoldUntil.IsZero() && time.Now().UTC().Before(tenantCfg.LegalHoldUntil)) {
			continue
		}
		summary := summaries[tenantID]
		if summary == nil {
			summary = &TenantCompactionSummary{
				TenantID:        tenantID,
				RetentionClass:  tenantCfg.Limits.RetentionClass,
				RetentionPeriod: tenantCfg.Limits.Retention.String(),
			}
			summaries[tenantID] = summary
		}
		summary.Scanned++
		resp.ScannedManifests++
		cutoff := time.Now().UTC().Add(-tenantCfg.Limits.Retention)
		if tenantCfg.Limits.Retention <= 0 || !record.End.Before(cutoff) {
			continue
		}
		summary.Deleted++
		resp.DeletedManifests++
		if req.DryRun {
			continue
		}
		deleteManifest, err := s.deleteManifest(ctx, tenantID, record, manifest, tenantCfg)
		if err != nil {
			return CompactResponse{}, err
		}
		if deleteManifest {
			resp.DeleteManifests++
		}
		resp.DeletedChunks += len(manifest.Chunks)
	}

	resp.ByTenant = make([]TenantCompactionSummary, 0, len(summaries))
	for _, summary := range summaries {
		resp.ByTenant = append(resp.ByTenant, *summary)
	}
	sort.Slice(resp.ByTenant, func(i, j int) bool { return resp.ByTenant[i].TenantID < resp.ByTenant[j].TenantID })
	return resp, nil
}

func (s *Server) manifestRecords(ctx context.Context, partitionKey string) ([]manifests.Record, error) {
	if s.manifests != nil {
		records, err := s.manifests.List(ctx, "segments/")
		if err != nil {
			return nil, err
		}
		if partitionKey == "" {
			return records, nil
		}
		filtered := make([]manifests.Record, 0, len(records))
		for _, record := range records {
			if strings.Contains(record.PartitionKey, partitionKey) {
				filtered = append(filtered, record)
			}
		}
		return filtered, nil
	}
	metas, err := s.objects.List(ctx, "segments/")
	if err != nil {
		return nil, err
	}
	records := make([]manifests.Record, 0, len(metas))
	for _, meta := range metas {
		data, _, err := s.objects.Get(ctx, meta.Key)
		if err != nil {
			return nil, err
		}
		records = append(records, manifests.Record{Key: meta.Key, Payload: data, CreatedAt: meta.CreatedAt})
	}
	return records, nil
}

func (s *Server) deleteManifest(ctx context.Context, tenantID string, record manifests.Record, manifest persistedSegmentManifest, tenantCfg model.TenantConfig) (bool, error) {
	deleteDoc := deleteManifestDocument{
		TenantID:       tenantID,
		RetentionClass: tenantCfg.Limits.RetentionClass,
		ManifestKey:    record.Key,
		Chunks:         manifest.Chunks,
		DeletedAt:      time.Now().UTC(),
		Reason:         "retention_expired",
	}
	deletePayload, err := json.MarshalIndent(deleteDoc, "", "  ")
	if err != nil {
		return false, err
	}
	key := deleteManifestKey(tenantID, deletePayload)
	if _, err := s.objects.Put(ctx, key, deletePayload); err != nil {
		return false, err
	}
	if s.manifests != nil {
		if _, err := s.manifests.Put(ctx, manifests.Record{
			Key:       key,
			Start:     record.Start,
			End:       record.End,
			Payload:   deletePayload,
			Checksum:  manifests.Checksum(deletePayload),
			CreatedAt: time.Now().UTC(),
		}); err != nil {
			return false, err
		}
	}
	for _, ch := range manifest.Chunks {
		if err := s.objects.Delete(ctx, ch.ObjectKey); err != nil {
			return false, err
		}
	}
	if err := s.objects.Delete(ctx, record.Key); err != nil {
		return false, err
	}
	if s.manifests != nil {
		if err := s.manifests.Delete(ctx, record.Key); err != nil {
			return false, err
		}
	}
	return true, nil
}

type persistedSegmentManifest struct {
	PartitionKey string              `json:"partition_key"`
	Start        time.Time           `json:"start"`
	End          time.Time           `json:"end"`
	Chunks       []persistedChunkRef `json:"chunks"`
	GeneratedAt  time.Time           `json:"generated_at"`
}

type persistedChunkRef struct {
	StreamKey string         `json:"stream_key"`
	ObjectKey string         `json:"object_key"`
	Checksum  string         `json:"checksum"`
	Start     time.Time      `json:"start"`
	End       time.Time      `json:"end"`
	Metadata  map[string]any `json:"metadata"`
}

type deleteManifestDocument struct {
	TenantID       string              `json:"tenant_id"`
	RetentionClass string              `json:"retention_class"`
	ManifestKey    string              `json:"manifest_key"`
	Chunks         []persistedChunkRef `json:"chunks"`
	DeletedAt      time.Time           `json:"deleted_at"`
	Reason         string              `json:"reason"`
}

func decodeManifest(ctx context.Context, data []byte, objects objectstore.Store, key string) (persistedSegmentManifest, error) {
	if len(data) == 0 && objects != nil {
		fetched, _, err := objects.Get(ctx, key)
		if err != nil {
			return persistedSegmentManifest{}, err
		}
		data = fetched
	}
	var manifest persistedSegmentManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return persistedSegmentManifest{}, err
	}
	return manifest, nil
}

func tenantIDFromManifest(manifest persistedSegmentManifest) string {
	for _, ch := range manifest.Chunks {
		if tenantID := tenantIDFromStreamKey(ch.StreamKey); tenantID != "" {
			return tenantID
		}
	}
	return "default"
}

func tenantIDFromStreamKey(streamKey string) string {
	if streamKey == "" {
		return ""
	}
	tenantID, _, ok := strings.Cut(streamKey, "|")
	if !ok {
		return streamKey
	}
	return tenantID
}

func deleteManifestKey(tenantID string, payload []byte) string {
	sum := fnv.New64a()
	_, _ = sum.Write(payload)
	return fmt.Sprintf("deletes/%s/%s-%016x.json", tenantID, time.Now().UTC().Format("20060102T150405.000000000Z"), sum.Sum64())
}

func buildTenantRepo(cfg config.Config) (tenant.Repository, error) {
	if cfg.TenantDSN != "" {
		return tenant.NewPostgresStore(context.Background(), cfg.TenantDSN)
	}
	return tenant.NewPersistentStore(filepath.Join(cfg.DataDir, "control", "tenants.json"))
}

func buildManifestRepo(cfg config.Config) (manifests.Repository, error) {
	if cfg.ManifestDSN != "" {
		return manifests.NewPostgresRepository(context.Background(), cfg.ManifestDSN)
	}
	return manifests.NewMemoryRepository(), nil
}

func decodeJSON(body io.ReadCloser, target any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
