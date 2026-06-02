package distributor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/coordination/ring"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/distribution"
	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

type Server struct {
	desc        servicehttp.Descriptor
	coordinator *ring.Coordinator
	router      *distribution.Router
	tenantStore *tenant.Store
	limiter     *distribution.Limiter
	localMember ring.Member
}

func NewServer(dataDir string) *Server {
	coordinator := ring.NewCoordinator(ring.NewMemoryBackend(), "/logagg/ring", 2)
	local := ring.Member{
		ID:      "distributor-1",
		Address: "127.0.0.1:9095",
		State:   ring.StateReady,
	}
	registered, _ := coordinator.Register(context.Background(), local, 30)
	tenantStore, _ := tenant.NewPersistentStore(filepath.Join(dataDir, "control", "tenants.json"))

	return &Server{
		desc:        Descriptor(),
		coordinator: coordinator,
		router:      distribution.NewRouter(coordinator),
		tenantStore: tenantStore,
		limiter:     distribution.NewLimiter(),
		localMember: registered,
	}
}

func NewServerWithCoordinator(coordinator *ring.Coordinator, local ring.Member) *Server {
	return &Server{
		desc:        Descriptor(),
		coordinator: coordinator,
		router:      distribution.NewRouter(coordinator),
		tenantStore: tenant.NewStoreWithDefaults(),
		limiter:     distribution.NewLimiter(),
		localMember: local,
	}
}

func NewServerWithDeps(coordinator *ring.Coordinator, local ring.Member, tenantStore *tenant.Store, limiter *distribution.Limiter) *Server {
	if tenantStore == nil {
		tenantStore = tenant.NewStoreWithDefaults()
	}
	if limiter == nil {
		limiter = distribution.NewLimiter()
	}
	return &Server{
		desc:        Descriptor(),
		coordinator: coordinator,
		router:      distribution.NewRouter(coordinator),
		tenantStore: tenantStore,
		limiter:     limiter,
		localMember: local,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	servicehttp.RegisterBaseRoutes(mux, s.desc)

	mux.HandleFunc("/internal/v1/append", s.handleRoute)
	mux.HandleFunc("/internal/v1/ring", s.handleRing)
	mux.HandleFunc("/internal/v1/members", s.handleMembers)
	mux.HandleFunc("/internal/v1/members/", s.handleMember)
	mux.HandleFunc("/internal/v1/assignment", s.handleAssignment)
	mux.HandleFunc("/internal/v1/rebalance", s.handleRebalance)

	return mux
}

type routeRequest struct {
	Event        model.Event `json:"event"`
	RequiredAcks int         `json:"required_acks"`
	Strict       bool        `json:"strict"`
}

func (s *Server) handleRoute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var req routeRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if req.Event.Timestamp.IsZero() {
		req.Event.Timestamp = time.Now().UTC()
	}
	if req.Event.TenantID == "" {
		req.Event.TenantID = "default"
	}

	if err := s.refreshTenantStore(r.Context()); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	tenantConfig, err := s.tenantStore.GetTenant(r.Context(), req.Event.TenantID)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		return
	}

	if err := req.Event.ValidateWithLimits(tenantConfig.Limits); err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	admission := s.limiter.Allow(tenantConfig, req.Event)
	if !admission.Allowed {
		if admission.RetryAfter > 0 {
			w.Header().Set("Retry-After", fmt.Sprintf("%.0f", admission.RetryAfter.Seconds()))
		}
		servicehttp.WriteJSON(w, http.StatusTooManyRequests, map[string]any{
			"error": admission.Reason,
			"backpressure": map[string]any{
				"allowed":     admission.Allowed,
				"reason":      admission.Reason,
				"retry_after": admission.RetryAfter.String(),
				"used_bytes":  admission.UsedBytes,
				"limit_bytes": admission.LimitBytes,
			},
		})
		return
	}

	result, err := s.router.Route(r.Context(), req.Event)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	ack := buildAck(result.Assignment, req.RequiredAcks, req.Strict)
	status := http.StatusOK
	if ack.PartialSuccess {
		status = http.StatusAccepted
		if req.Strict {
			status = http.StatusServiceUnavailable
		}
	}

	servicehttp.WriteJSON(w, status, map[string]any{
		"event":          result.Event,
		"assignment":     result.Assignment,
		"acknowledgment": ack,
		"backpressure": map[string]any{
			"allowed":     true,
			"reason":      "",
			"retry_after": "0s",
			"used_bytes":  admission.UsedBytes,
			"limit_bytes": admission.LimitBytes,
		},
	})
}

func (s *Server) handleRing(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := s.refreshTenantStore(r.Context()); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	snapshot, err := s.coordinator.Snapshot(r.Context())
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(snapshot)
}

func (s *Server) handleMembers(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if err := s.refreshTenantStore(r.Context()); err != nil {
			servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		members, err := s.coordinator.List(r.Context())
		if err != nil {
			servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
			"members":      members,
			"local_member": s.localMember,
		})
	case http.MethodPost:
		if err := s.refreshTenantStore(r.Context()); err != nil {
			servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		var req struct {
			Member ring.Member `json:"member"`
			TTL    int64       `json:"ttl"`
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if req.TTL <= 0 {
			req.TTL = 30
		}
		member, err := s.coordinator.Register(r.Context(), req.Member, req.TTL)
		if err != nil {
			servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusCreated, map[string]any{
			"member": member,
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleMember(w http.ResponseWriter, r *http.Request) {
	memberID, ok := parseMemberID(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodDelete:
		if err := s.refreshTenantStore(r.Context()); err != nil {
			servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if err := s.coordinator.Delete(r.Context(), memberID); err != nil {
			servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusOK, map[string]any{"deleted": memberID})
	case http.MethodPut:
		if err := s.refreshTenantStore(r.Context()); err != nil {
			servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		var req struct {
			State ring.State `json:"state"`
			TTL   int64      `json:"ttl"`
		}
		if err := decodeJSON(r.Body, &req); err != nil {
			servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if req.TTL <= 0 {
			req.TTL = 30
		}
		member, err := s.coordinator.SetState(r.Context(), memberID, req.State, req.TTL)
		if err != nil {
			servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		servicehttp.WriteJSON(w, http.StatusOK, map[string]any{"member": member})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleAssignment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := s.refreshTenantStore(r.Context()); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	streamKey := r.URL.Query().Get("stream_key")
	if streamKey == "" {
		servicehttp.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "stream_key is required"})
		return
	}
	assignment, err := s.coordinator.Resolve(r.Context(), streamKey)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, map[string]any{
		"assignment": assignment,
	})
}

func (s *Server) handleRebalance(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if err := s.refreshTenantStore(r.Context()); err != nil {
		servicehttp.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	tokenCount := 0
	if raw := r.URL.Query().Get("token_count"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil {
			tokenCount = parsed
		}
	}
	report, err := s.coordinator.Rebalance(r.Context(), tokenCount)
	if err != nil {
		servicehttp.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": err.Error()})
		return
	}
	servicehttp.WriteJSON(w, http.StatusOK, report)
}

func (s *Server) refreshTenantStore(ctx context.Context) error {
	if s.tenantStore == nil {
		return nil
	}
	_, err := s.tenantStore.ReloadIfModified(ctx)
	return err
}

func decodeJSON(body io.ReadCloser, target any) error {
	defer body.Close()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func parseMemberID(path string) (string, bool) {
	const prefix = "/internal/v1/members/"
	if len(path) <= len(prefix) || path[:len(prefix)] != prefix {
		return "", false
	}
	return path[len(prefix):], true
}

func buildAck(assignment ring.Assignment, requiredAcks int, strict bool) distribution.Acknowledgment {
	acked := []ring.Member{assignment.Primary}
	acked = append(acked, assignment.Replicas...)
	available := len(acked)
	if requiredAcks <= 0 {
		requiredAcks = available
	}

	ack := distribution.Acknowledgment{
		RequiredAcks:  requiredAcks,
		AvailableAcks: available,
		AckedMembers:  acked,
		Strict:        strict,
		Status:        "acked",
	}
	if available < requiredAcks {
		ack.PartialSuccess = true
		ack.Status = "partial"
		ack.MissingMembers = []ring.Member{}
		return ack
	}
	return ack
}
