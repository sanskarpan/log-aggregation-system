package controlplane

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "control-plane",
		Role:       "Stores tenant policies, schemas, parser pipelines, and placement metadata.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"postgres",
			"etcd",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodGet, Path: "/api/admin/tenants", State: "implemented-file-backed", Description: "Lists persisted tenant configs from the local control-plane snapshot."},
			{Method: http.MethodPost, Path: "/api/admin/tenants", State: "implemented-file-backed", Description: "Creates tenants and default policy sets."},
			{Method: http.MethodPut, Path: "/api/admin/tenants/{id}/limits", State: "implemented-file-backed", Description: "Updates tenant ingestion, query, and retention budgets."},
			{Method: http.MethodPut, Path: "/api/admin/tenants/{id}/pipelines", State: "implemented-file-backed", Description: "Replaces parser pipeline configuration for a tenant."},
			{Method: http.MethodPut, Path: "/api/admin/tenants/{id}/searchable-fields", State: "implemented-file-backed", Description: "Updates the structured-field indexing allowlist."},
		},
	}
}
