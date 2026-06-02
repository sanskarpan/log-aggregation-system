package compactor

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "compactor",
		Role:       "Compacts index segments, rebuilds bloom filters, and applies retention policies.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"object-storage",
			"postgres",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/compact", State: "implemented-retention", Description: "Scans manifests, applies retention cutoffs, and emits delete manifests."},
			{Method: http.MethodGet, Path: "/internal/v1/status", State: "implemented-retention", Description: "Returns compactor readiness and data directory information."},
		},
	}
}
