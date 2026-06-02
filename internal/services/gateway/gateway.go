package gateway

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "gateway",
		Role:       "External ingest and query entrypoint with auth, tenancy, and compatibility routing.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"control-plane",
			"distributor",
			"query-frontend",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/api/native/v1/ingest", State: "implemented-single-node", Description: "Native single-node ingest endpoint backed by the local engine."},
			{Method: http.MethodPost, Path: "/api/native/v1/search", State: "implemented-single-node", Description: "Native single-node query endpoint for label, field, and text filters."},
			{Method: http.MethodPost, Path: "/api/native/v1/tail", State: "implemented-single-node", Description: "Native single-node tail endpoint for recent matching logs."},
			{Method: http.MethodPost, Path: "/api/native/v1/explain", State: "implemented-single-node", Description: "Native single-node query planner explanation endpoint."},
			{Method: http.MethodGet, Path: "/api/admin/tenants", State: "implemented-single-node", Description: "Lists locally configured tenants and policy defaults."},
			{Method: http.MethodGet, Path: "/api/admin/queue", State: "implemented-local-queue", Description: "Returns local queue topic, offset, and lag stats."},
			{Method: http.MethodPost, Path: "/otlp/v1/logs", State: "implemented-single-node", Description: "OTLP HTTP ingest for log exports backed by the local engine."},
			{Method: http.MethodPost, Path: "/loki/api/v1/push", State: "implemented-single-node", Description: "Loki-compatible write path backed by the local engine."},
			{Method: http.MethodGet, Path: "/api/v1/query", State: "implemented-single-node", Description: "Instant query compatibility endpoint."},
			{Method: http.MethodGet, Path: "/api/v1/query_range", State: "implemented-single-node", Description: "Range query compatibility endpoint."},
		},
	}
}
