package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

type Server struct {
	desc        servicehttp.Descriptor
	tenantStore tenant.Repository
	snapshot    string
	auditPath   string
}

func NewServer(dataDir string) (*Server, error) {
	snapshot := filepath.Join(dataDir, "control", "tenants.json")
	var store tenant.Repository
	var err error
	if dsn := strings.TrimSpace(os.Getenv("TENANT_DSN")); dsn != "" {
		store, err = tenant.NewPostgresStore(context.Background(), dsn)
		if err != nil {
			return nil, err
		}
	} else {
		store, err = tenant.NewPersistentStore(snapshot)
		if err != nil {
			return nil, err
		}
	}
	return &Server{
		desc:        Descriptor(),
		tenantStore: store,
		snapshot:    snapshot,
		auditPath:   filepath.Join(dataDir, "control", "audit.log"),
	}, nil
}

func NewServerWithStore(store tenant.Repository) *Server {
	if store == nil {
		store = tenant.NewStoreWithDefaults()
	}
	return &Server{
		desc:        Descriptor(),
		tenantStore: store,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	servicehttp.RegisterBaseRoutes(mux, s.desc)
	mux.HandleFunc("/api/admin/tenants", s.handleTenants)
	mux.HandleFunc("/api/admin/tenants/", s.handleTenantMutation)
	return mux
}

func (s *Server) handleTenants(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.refreshTenantStore(r.Context())
		servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
			"snapshot_path": s.snapshot,
			"count":         len(s.tenantStore.List(r.Context())),
			"tenants":       s.tenantStore.List(r.Context()),
		})
	case http.MethodPost:
		s.createTenant(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleTenantMutation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	tenantID, action, ok := parseTenantAction(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	s.refreshTenantStore(r.Context())

	switch action {
	case "limits":
		s.updateLimits(w, r, tenantID)
	case "pipelines":
		s.updatePipelines(w, r, tenantID)
	case "searchable-fields":
		s.updateSearchableFields(w, r, tenantID)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) createTenant(w http.ResponseWriter, r *http.Request) {
	req, err := decodeTenantConfig(r.Body)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	tenantConfig := tenant.DefaultTenantConfig()
	tenantConfig.ID = strings.TrimSpace(req.ID)
	if tenantConfig.ID == "" {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "tenant id is required"})
		return
	}
	if strings.TrimSpace(req.Name) != "" {
		tenantConfig.Name = req.Name
	}
	if len(req.SearchableFields) > 0 {
		tenantConfig.SearchableFields = cloneStrings(req.SearchableFields)
	}
	if len(req.ReservedLabels) > 0 {
		tenantConfig.ReservedLabels = cloneStrings(req.ReservedLabels)
	}
	if len(req.LabelPromotionAllow) > 0 {
		tenantConfig.LabelPromotionAllow = cloneStrings(req.LabelPromotionAllow)
	}
	if len(req.LabelPromotionDeny) > 0 {
		tenantConfig.LabelPromotionDeny = cloneStrings(req.LabelPromotionDeny)
	}
	if len(req.StructuredFieldPolicy) > 0 {
		tenantConfig.StructuredFieldPolicy = cloneStringMap(req.StructuredFieldPolicy)
	}
	if len(req.Pipelines) > 0 {
		tenantConfig.Pipelines = clonePipelines(req.Pipelines)
	}
	if req.ActivePipelineID != "" {
		tenantConfig.ActivePipelineID = req.ActivePipelineID
	}
	if req.Limits != (model.TenantLimits{}) {
		tenantConfig.Limits = req.Limits
	}

	if err := validateTenantPolicyCompatibility(nil, tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "create_tenant", tenantConfig.ID, nil, &tenantConfig, "rejected", err)
		return
	}
	if err := s.tenantStore.Put(r.Context(), tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "create_tenant", tenantConfig.ID, nil, &tenantConfig, "error", err)
		return
	}
	_ = s.auditMutation(r.Context(), "create_tenant", tenantConfig.ID, nil, &tenantConfig, "accepted", nil)

	servicehttp.WriteJSON(w, http.StatusCreated, map[string]any{
		"tenant": tenantConfig,
	})
}

func (s *Server) updateLimits(w http.ResponseWriter, r *http.Request, tenantID string) {
	req, err := decodeTenantLimits(r.Body)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	tenantConfig, err := s.tenantStore.GetTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			servicehttp.WriteJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	tenantConfig.Limits = req

	if err := validateTenantPolicyCompatibility(nil, tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "update_limits", tenantID, nil, &tenantConfig, "rejected", err)
		return
	}
	if err := s.tenantStore.Put(r.Context(), tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "update_limits", tenantID, nil, &tenantConfig, "error", err)
		return
	}
	_ = s.auditMutation(r.Context(), "update_limits", tenantID, nil, &tenantConfig, "accepted", nil)

	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant": tenantConfig,
	})
}

func (s *Server) updatePipelines(w http.ResponseWriter, r *http.Request, tenantID string) {
	req, err := decodeTenantPipelines(r.Body)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	tenantConfig, err := s.tenantStore.GetTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			servicehttp.WriteJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	before := tenantConfig
	tenantConfig.Pipelines = clonePipelines(req.Pipelines)
	tenantConfig.ActivePipelineID = req.ActivePipelineID

	if err := validateTenantPolicyCompatibility(&before, tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "update_pipelines", tenantID, &before, &tenantConfig, "rejected", err)
		return
	}
	if err := s.tenantStore.Put(r.Context(), tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "update_pipelines", tenantID, &before, &tenantConfig, "error", err)
		return
	}
	_ = s.auditMutation(r.Context(), "update_pipelines", tenantID, &before, &tenantConfig, "accepted", nil)

	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant": tenantConfig,
	})
}

func (s *Server) updateSearchableFields(w http.ResponseWriter, r *http.Request, tenantID string) {
	req, err := decodeSearchableFields(r.Body)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	tenantConfig, err := s.tenantStore.GetTenant(r.Context(), tenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			servicehttp.WriteJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	before := tenantConfig
	tenantConfig.SearchableFields = cloneStrings(req.SearchableFields)

	if err := validateTenantPolicyCompatibility(&before, tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "update_searchable_fields", tenantID, &before, &tenantConfig, "rejected", err)
		return
	}
	if err := s.tenantStore.Put(r.Context(), tenantConfig); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		_ = s.auditMutation(r.Context(), "update_searchable_fields", tenantID, &before, &tenantConfig, "error", err)
		return
	}
	_ = s.auditMutation(r.Context(), "update_searchable_fields", tenantID, &before, &tenantConfig, "accepted", nil)

	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"tenant": tenantConfig,
	})
}

func (s *Server) refreshTenantStore(ctx context.Context) {
	if s.tenantStore == nil {
		return
	}
	_, _ = s.tenantStore.ReloadIfModified(ctx)
}

func (s *Server) auditMutation(ctx context.Context, action, tenantID string, before, after *model.TenantConfig, outcome string, err error) error {
	if s.auditPath == "" {
		return nil
	}

	record := auditRecord{
		Timestamp: time.Now().UTC(),
		Action:    action,
		TenantID:  tenantID,
		Outcome:   outcome,
	}
	if before != nil {
		record.Before = before
	}
	if after != nil {
		record.After = after
	}
	if err != nil {
		record.Error = err.Error()
	}

	data, marshalErr := json.Marshal(record)
	if marshalErr != nil {
		return marshalErr
	}

	if err := os.MkdirAll(filepath.Dir(s.auditPath), 0o755); err != nil {
		return err
	}

	file, err := os.OpenFile(s.auditPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

type auditRecord struct {
	Timestamp time.Time           `json:"timestamp"`
	Action    string              `json:"action"`
	TenantID  string              `json:"tenant_id"`
	Outcome   string              `json:"outcome"`
	Error     string              `json:"error,omitempty"`
	Before    *model.TenantConfig `json:"before,omitempty"`
	After     *model.TenantConfig `json:"after,omitempty"`
}

func validateTenantPolicyCompatibility(before *model.TenantConfig, after model.TenantConfig) error {
	if err := after.Validate(); err != nil {
		return err
	}
	if err := validatePipelineCompatibility(after.Pipelines, after.ActivePipelineID); err != nil {
		return err
	}
	if err := validateSearchableFieldCompatibility(before, after); err != nil {
		return err
	}
	return nil
}

func validatePipelineCompatibility(pipelines []model.ParsePipeline, activeID string) error {
	ids := make(map[string]struct{}, len(pipelines))
	for _, pipeline := range pipelines {
		if strings.TrimSpace(pipeline.ID) == "" {
			return fmt.Errorf("pipeline id is required")
		}
		if _, ok := ids[pipeline.ID]; ok {
			return fmt.Errorf("duplicate pipeline id %q", pipeline.ID)
		}
		ids[pipeline.ID] = struct{}{}
		for _, stage := range pipeline.Stages {
			if err := validatePipelineStage(stage); err != nil {
				return fmt.Errorf("pipeline %q: %w", pipeline.ID, err)
			}
		}
	}
	if len(pipelines) == 0 {
		if strings.TrimSpace(activeID) != "" {
			return fmt.Errorf("active_pipeline_id must be empty when no pipelines are configured")
		}
		return nil
	}
	if _, ok := ids[activeID]; !ok {
		return fmt.Errorf("active_pipeline_id %q does not match any configured pipeline", activeID)
	}
	return nil
}

func validatePipelineStage(stage model.PipelineStage) error {
	switch strings.ToLower(strings.TrimSpace(stage.Type)) {
	case "json", "logfmt", "regex", "static", "timestamp", "severity", "drop", "rename", "redact":
		return nil
	default:
		return fmt.Errorf("unsupported pipeline stage type %q", stage.Type)
	}
}

func validateSearchableFieldCompatibility(before *model.TenantConfig, after model.TenantConfig) error {
	if len(after.SearchableFields) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, field := range after.SearchableFields {
		field = strings.TrimSpace(field)
		if field == "" {
			return fmt.Errorf("searchable field cannot be empty")
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("duplicate searchable field %q", field)
		}
		seen[field] = struct{}{}
	}
	if before == nil || len(before.SearchableFields) == 0 {
		return nil
	}

	intersection := 0
	beforeSet := map[string]struct{}{}
	for _, field := range before.SearchableFields {
		beforeSet[field] = struct{}{}
	}
	for field := range seen {
		if _, ok := beforeSet[field]; ok {
			intersection++
		}
	}
	if intersection == 0 {
		return fmt.Errorf("searchable field update must preserve at least one existing field")
	}
	return nil
}

func parseTenantAction(path string) (string, string, bool) {
	trimmed := strings.TrimPrefix(path, "/api/admin/tenants/")
	trimmed = strings.Trim(trimmed, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}

type createTenantRequest = model.TenantConfig

type pipelinesUpdateRequest struct {
	Pipelines        []model.ParsePipeline `json:"pipelines"`
	ActivePipelineID string                `json:"active_pipeline_id"`
}

type searchableFieldsUpdateRequest struct {
	SearchableFields []string `json:"searchable_fields"`
}

func decodeTenantConfig(body io.ReadCloser) (createTenantRequest, error) {
	defer body.Close()
	var req createTenantRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return createTenantRequest{}, err
	}
	return req, nil
}

func decodeTenantLimits(body io.ReadCloser) (model.TenantLimits, error) {
	defer body.Close()
	var req model.TenantLimits
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return model.TenantLimits{}, err
	}
	return req, nil
}

func decodeTenantPipelines(body io.ReadCloser) (pipelinesUpdateRequest, error) {
	defer body.Close()
	var req pipelinesUpdateRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return pipelinesUpdateRequest{}, err
	}
	return req, nil
}

func decodeSearchableFields(body io.ReadCloser) (searchableFieldsUpdateRequest, error) {
	defer body.Close()
	var req searchableFieldsUpdateRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return searchableFieldsUpdateRequest{}, err
	}
	return req, nil
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, len(values))
	copy(out, values)
	return out
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func clonePipelines(values []model.ParsePipeline) []model.ParsePipeline {
	if len(values) == 0 {
		return nil
	}
	out := make([]model.ParsePipeline, len(values))
	for i, pipeline := range values {
		out[i] = pipeline
		if len(pipeline.Stages) > 0 {
			stages := make([]model.PipelineStage, len(pipeline.Stages))
			copy(stages, pipeline.Stages)
			out[i].Stages = stages
		}
	}
	return out
}
