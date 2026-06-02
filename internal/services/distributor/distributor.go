package distributor

import (
	"net/http"

	"github.com/sanskar/log-aggregation-system/internal/platform/servicehttp"
)

func Descriptor() servicehttp.Descriptor {
	return servicehttp.Descriptor{
		Name:       "distributor",
		Role:       "Validates writes, enforces tenant budgets, and routes streams to ingesters.",
		Version:    "0.1.0-dev",
		Deployment: []string{"kubernetes", "bare-metal"},
		Dependencies: []string{
			"etcd",
			"ring",
		},
		Endpoints: []servicehttp.Endpoint{
			{Method: http.MethodPost, Path: "/internal/v1/append", State: "implemented-single-node", Description: "Routes a canonical event to a primary and replica set."},
			{Method: http.MethodGet, Path: "/internal/v1/ring", State: "implemented-single-node", Description: "Returns the current ring snapshot for debugging."},
			{Method: http.MethodGet, Path: "/internal/v1/members", State: "implemented-single-node", Description: "Returns current ring members."},
			{Method: http.MethodPost, Path: "/internal/v1/members", State: "implemented-single-node", Description: "Registers or refreshes a ring member for multi-node routing."},
			{Method: http.MethodPut, Path: "/internal/v1/members/{id}", State: "implemented-single-node", Description: "Updates a ring member state for draining or leaving."},
			{Method: http.MethodDelete, Path: "/internal/v1/members/{id}", State: "implemented-single-node", Description: "Deletes a ring member to simulate failure or removal."},
		},
	}
}
