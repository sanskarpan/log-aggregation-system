package distribution

import (
	"context"

	"github.com/sanskar/log-aggregation-system/internal/coordination/ring"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type Router struct {
	coordinator *ring.Coordinator
}

type RouteResult struct {
	Event      model.Event     `json:"event"`
	Assignment ring.Assignment `json:"assignment"`
}

func NewRouter(coordinator *ring.Coordinator) *Router {
	return &Router{coordinator: coordinator}
}

func (r *Router) Route(ctx context.Context, event model.Event) (RouteResult, error) {
	assignment, err := r.coordinator.Resolve(ctx, event.StreamKey())
	if err != nil {
		return RouteResult{}, err
	}
	return RouteResult{
		Event:      event,
		Assignment: assignment,
	}, nil
}
