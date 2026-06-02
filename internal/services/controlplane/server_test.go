package controlplane

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
	"github.com/sanskar/log-aggregation-system/internal/engine/tenant"
	distributorsvc "github.com/sanskar/log-aggregation-system/internal/services/distributor"
)

func TestControlPlaneTenantLifecycle(t *testing.T) {
	dir := t.TempDir()
	server, err := NewServer(dir)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	handler := server.Handler()

	createPayload, _ := json.Marshal(tenant.DefaultTenantConfig())
	createPayloadMap := map[string]any{}
	if err := json.Unmarshal(createPayload, &createPayloadMap); err != nil {
		t.Fatalf("decode tenant payload: %v", err)
	}
	createPayloadMap["id"] = "tenant-a"
	createPayloadMap["name"] = "Tenant A"
	createPayloadMap["searchable_fields"] = []string{"service", "status_code"}
	createBody, _ := json.Marshal(createPayloadMap)
	createReq := httptest.NewRequest(http.MethodPost, "/api/admin/tenants", bytes.NewReader(createBody))
	createRes := httptest.NewRecorder()
	handler.ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("unexpected create status: %d", createRes.Code)
	}

	limitsPayload := model.TenantLimits{
		IngestRateMBPerSecond: 1,
		QueryConcurrency:      4,
		MaxLabelsPerStream:    4,
		MaxBodyBytes:          128,
		MaxParsedFields:       8,
		MaxFieldValueBytes:    64,
		Retention:             time.Hour,
	}
	limitsBody, _ := json.Marshal(limitsPayload)
	limitsReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/tenant-a/limits", bytes.NewReader(limitsBody))
	limitsRes := httptest.NewRecorder()
	handler.ServeHTTP(limitsRes, limitsReq)
	if limitsRes.Code != http.StatusOK {
		t.Fatalf("unexpected limits status: %d", limitsRes.Code)
	}

	pipelinesPayload := map[string]any{
		"pipelines": []model.ParsePipeline{
			{
				ID:          "default",
				Description: "JSON pipeline",
				Stages: []model.PipelineStage{
					{Name: "json", Type: "json", Config: map[string]string{"source": "body"}},
				},
			},
		},
		"active_pipeline_id": "default",
	}
	pipelinesBody, _ := json.Marshal(pipelinesPayload)
	pipelinesReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/tenant-a/pipelines", bytes.NewReader(pipelinesBody))
	pipelinesRes := httptest.NewRecorder()
	handler.ServeHTTP(pipelinesRes, pipelinesReq)
	if pipelinesRes.Code != http.StatusOK {
		t.Fatalf("unexpected pipelines status: %d", pipelinesRes.Code)
	}

	fieldsBody, _ := json.Marshal(map[string]any{
		"searchable_fields": []string{"service", "status_code", "host"},
	})
	fieldsReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/tenant-a/searchable-fields", bytes.NewReader(fieldsBody))
	fieldsRes := httptest.NewRecorder()
	handler.ServeHTTP(fieldsRes, fieldsReq)
	if fieldsRes.Code != http.StatusOK {
		t.Fatalf("unexpected searchable-fields status: %d", fieldsRes.Code)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/api/admin/tenants", nil)
	listRes := httptest.NewRecorder()
	handler.ServeHTTP(listRes, listReq)
	if listRes.Code != http.StatusOK {
		t.Fatalf("unexpected list status: %d", listRes.Code)
	}

	var listResponse struct {
		Tenants []model.TenantConfig `json:"tenants"`
	}
	if err := json.Unmarshal(listRes.Body.Bytes(), &listResponse); err != nil {
		t.Fatalf("decode tenant list: %v", err)
	}
	found := false
	for _, tenantConfig := range listResponse.Tenants {
		if tenantConfig.ID == "tenant-a" {
			found = true
			if len(tenantConfig.Pipelines) != 1 || tenantConfig.ActivePipelineID != "default" {
				t.Fatalf("unexpected pipeline config: %+v", tenantConfig)
			}
			if len(tenantConfig.SearchableFields) != 3 {
				t.Fatalf("unexpected searchable fields: %+v", tenantConfig.SearchableFields)
			}
		}
	}
	if !found {
		t.Fatalf("created tenant not returned from list")
	}

	auditPath := dir + "/control/audit.log"
	data, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 4 {
		t.Fatalf("expected audit records for all policy writes, got %d lines: %s", len(lines), string(data))
	}
	if !strings.Contains(string(data), `"action":"create_tenant"`) || !strings.Contains(string(data), `"outcome":"accepted"`) {
		t.Fatalf("expected accepted create_tenant audit record, got %s", string(data))
	}
}

func TestControlPlaneCompatibilityGuards(t *testing.T) {
	server, err := NewServer(t.TempDir())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	handler := server.Handler()

	createBody, _ := json.Marshal(map[string]any{
		"id":   "tenant-b",
		"name": "Tenant B",
	})
	createReq := httptest.NewRequest(http.MethodPost, "/api/admin/tenants", bytes.NewReader(createBody))
	createRes := httptest.NewRecorder()
	handler.ServeHTTP(createRes, createReq)
	if createRes.Code != http.StatusCreated {
		t.Fatalf("unexpected create status: %d", createRes.Code)
	}

	badPipelineBody, _ := json.Marshal(map[string]any{
		"pipelines": []model.ParsePipeline{
			{
				ID: "default",
				Stages: []model.PipelineStage{
					{Name: "bad", Type: "unknown"},
				},
			},
		},
		"active_pipeline_id": "default",
	})
	badPipelineReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/tenant-b/pipelines", bytes.NewReader(badPipelineBody))
	badPipelineRes := httptest.NewRecorder()
	handler.ServeHTTP(badPipelineRes, badPipelineReq)
	if badPipelineRes.Code != http.StatusBadRequest {
		t.Fatalf("expected pipeline compatibility rejection, got %d", badPipelineRes.Code)
	}

	fieldsBody, _ := json.Marshal(map[string]any{
		"searchable_fields": []string{"service", "status_code"},
	})
	fieldsReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/tenant-b/searchable-fields", bytes.NewReader(fieldsBody))
	fieldsRes := httptest.NewRecorder()
	handler.ServeHTTP(fieldsRes, fieldsReq)
	if fieldsRes.Code != http.StatusOK {
		t.Fatalf("unexpected searchable-fields setup status: %d", fieldsRes.Code)
	}

	rejectFieldsBody, _ := json.Marshal(map[string]any{
		"searchable_fields": []string{"host"},
	})
	rejectFieldsReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/tenant-b/searchable-fields", bytes.NewReader(rejectFieldsBody))
	rejectFieldsRes := httptest.NewRecorder()
	handler.ServeHTTP(rejectFieldsRes, rejectFieldsReq)
	if rejectFieldsRes.Code != http.StatusBadRequest {
		t.Fatalf("expected searchable field compatibility rejection, got %d", rejectFieldsRes.Code)
	}
}

func TestDistributorRefreshesTenantSnapshotFromControlPlane(t *testing.T) {
	dir := t.TempDir()
	controlServer, err := NewServer(dir)
	if err != nil {
		t.Fatalf("new control-plane server: %v", err)
	}
	controlHandler := controlServer.Handler()

	distributorServer := distributorsvc.NewServer(dir)
	distributorHandler := distributorServer.Handler()

	limitsPayload := model.TenantLimits{
		IngestRateMBPerSecond: 1,
		QueryConcurrency:      4,
		MaxLabelsPerStream:    4,
		MaxBodyBytes:          8,
		MaxParsedFields:       8,
		MaxFieldValueBytes:    64,
		Retention:             time.Hour,
	}
	limitsBody, _ := json.Marshal(limitsPayload)
	limitsReq := httptest.NewRequest(http.MethodPut, "/api/admin/tenants/default/limits", bytes.NewReader(limitsBody))
	limitsRes := httptest.NewRecorder()
	controlHandler.ServeHTTP(limitsRes, limitsReq)
	if limitsRes.Code != http.StatusOK {
		t.Fatalf("unexpected limits status: %d", limitsRes.Code)
	}

	payload, _ := json.Marshal(map[string]any{
		"event": model.Event{
			Timestamp:    time.Now().UTC(),
			TenantID:     "default",
			StreamLabels: map[string]string{"service": "payments"},
			Body:         "0123456789abcdef",
		},
	})
	appendReq := httptest.NewRequest(http.MethodPost, "/internal/v1/append", bytes.NewReader(payload))
	appendRes := httptest.NewRecorder()
	distributorHandler.ServeHTTP(appendRes, appendReq)
	if appendRes.Code != http.StatusBadRequest {
		t.Fatalf("expected refreshed limits to reject event, got %d", appendRes.Code)
	}
}
