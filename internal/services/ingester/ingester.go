package ingester

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "ingester",
		Role:       "Owns WAL, stream chunking, replication, and flush lifecycle.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"object-storage",
			"indexer",
			"kafka",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/ingest", State: "planned", Description: "Appends validated events to WAL-backed active streams."},
		},
	}
}
