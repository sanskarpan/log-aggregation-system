package querier

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "querier",
		Role:       "Fetches index segments and chunks, then executes plans against hot and cold data.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"object-storage",
			"redis",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/execute", State: "implemented-single-node", Description: "Executes query fragments and returns merged shard results."},
		},
	}
}
