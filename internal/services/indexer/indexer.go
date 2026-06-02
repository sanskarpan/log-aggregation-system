package indexer

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "indexer",
		Role:       "Builds label, structured-field, and bloom-accelerated index segments.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"object-storage",
			"postgres",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/index", State: "planned", Description: "Creates immutable index segments for flushed chunks."},
		},
	}
}
