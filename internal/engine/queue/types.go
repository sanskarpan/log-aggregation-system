package queue

import (
	"context"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

const (
	TopicNormalizedEvents = "logs.normalized.v1"
	TopicRetryEvents      = "logs.normalized.retry.v1"
	TopicDeadLetter       = "logs.normalized.dlq.v1"
)

type Message struct {
	Topic     string      `json:"topic"`
	Partition int         `json:"partition"`
	Offset    int64       `json:"offset"`
	Key       string      `json:"key"`
	Event     model.Event `json:"event"`
	Attempts  int         `json:"attempts"`
	LastError string      `json:"last_error,omitempty"`
	CreatedAt time.Time   `json:"created_at"`
}

type PublishRequest struct {
	Topic     string      `json:"topic"`
	Key       string      `json:"key"`
	Event     model.Event `json:"event"`
	Attempts  int         `json:"attempts"`
	LastError string      `json:"last_error,omitempty"`
}

type TopicStats struct {
	Topic         string           `json:"topic"`
	Partitions    int              `json:"partitions"`
	HighWatermark map[int]int64    `json:"high_watermark"`
	Committed     map[string]int64 `json:"committed"`
	Lag           map[string]int64 `json:"lag"`
	Messages      int              `json:"messages"`
}

type Broker interface {
	Publish(ctx context.Context, req PublishRequest) (Message, error)
	Poll(ctx context.Context, group, topic string, limit int) ([]Message, error)
	Commit(ctx context.Context, group string, msg Message) error
	Stats(ctx context.Context) []TopicStats
}

type ReplayableBroker interface {
	Broker
	Replay(ctx context.Context, topic string, limit int) ([]Message, error)
}

type Handler interface {
	Handle(ctx context.Context, msg Message) error
}

type HandlerFunc func(ctx context.Context, msg Message) error

func (f HandlerFunc) Handle(ctx context.Context, msg Message) error {
	return f(ctx, msg)
}
