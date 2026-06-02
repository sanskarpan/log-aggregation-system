package ports

import (
	"context"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type Parser interface {
	Process(ctx context.Context, tenant model.TenantConfig, event model.Event) (model.Event, error)
}

type Distributor interface {
	Route(ctx context.Context, tenant model.TenantConfig, event model.Event) (string, error)
}

type Ingester interface {
	Append(ctx context.Context, event model.Event) error
}

type Indexer interface {
	Index(ctx context.Context, event model.Event) error
}

type QueryPlanner interface {
	Plan(ctx context.Context, req model.QueryRequest) (string, error)
}

type QueryExecutor interface {
	Execute(ctx context.Context, req model.QueryRequest) (model.QueryResult, error)
}

type TenantStore interface {
	GetTenant(ctx context.Context, tenantID string) (model.TenantConfig, error)
}
