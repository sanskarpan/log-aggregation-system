package queryfrontend

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "query-frontend",
		Role:       "Plans, splits, caches, and merges user-facing queries.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"querier",
			"redis",
			"control-plane",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/plan", State: "implemented-single-node", Description: "Generates executable plans for range, tail, and field-aware search queries."},
			{Method: http.MethodPost, Path: "/internal/v1/query", State: "implemented-single-node", Description: "Plans, fans out, and merges query fragments across querier workers."},
			{Method: http.MethodGet, Path: "/api/admin/cache", State: "implemented-single-node", Description: "Returns query and chunk cache statistics."},
		},
	}
}
