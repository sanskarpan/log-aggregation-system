package parser

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "parser",
		Role:       "Executes tenant-configured parsing, enrichment, and label promotion pipelines.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"control-plane",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/process", State: "planned", Description: "Normalizes events and extracts searchable fields."},
		},
	}
}
