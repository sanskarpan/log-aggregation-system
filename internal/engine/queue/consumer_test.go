package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

func TestMemoryBrokerPublishPollCommitAndLag(t *testing.T) {
	ctx := context.Background()
	broker := NewMemoryBroker(2)
	event := model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "default",
		Body:      "hello",
	}

	if _, err := broker.Publish(ctx, PublishRequest{
		Topic: TopicNormalizedEvents,
		Key:   event.StreamKey(),
		Event: event,
	}); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	messages, err := broker.Poll(ctx, "writers", TopicNormalizedEvents, 10)
	if err != nil {
		t.Fatalf("poll event: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(messages))
	}

	stats := broker.Stats(ctx)
	if lagFor(stats, "writers:"+TopicNormalizedEvents) != 1 {
		t.Fatalf("expected lag 1 before commit, got %+v", stats)
	}

	if err := broker.Commit(ctx, "writers", messages[0]); err != nil {
		t.Fatalf("commit event: %v", err)
	}
	if lagFor(broker.Stats(ctx), "writers:"+TopicNormalizedEvents) != 0 {
		t.Fatalf("expected lag 0 after commit, got %+v", broker.Stats(ctx))
	}
}

func TestConsumerRetryAndDeadLetter(t *testing.T) {
	ctx := context.Background()
	broker := NewMemoryBroker(1)
	event := model.Event{
		Timestamp: time.Now().UTC(),
		TenantID:  "default",
		Body:      "bad",
	}
	if _, err := broker.Publish(ctx, PublishRequest{Topic: TopicNormalizedEvents, Key: event.StreamKey(), Event: event}); err != nil {
		t.Fatalf("publish event: %v", err)
	}

	consumer := NewConsumer(broker, HandlerFunc(func(context.Context, Message) error {
		return errors.New("poison message")
	}), ConsumerOptions{
		Group:       "writers",
		MaxAttempts: 2,
		BatchSize:   10,
	})

	first, err := consumer.ConsumeOnce(ctx, 10)
	if err != nil {
		t.Fatalf("consume first batch: %v", err)
	}
	if first.Retried != 1 || first.DeadLettered != 0 {
		t.Fatalf("unexpected first report: %+v", first)
	}

	second, err := consumer.ConsumeOnce(ctx, 10)
	if err != nil {
		t.Fatalf("consume second batch: %v", err)
	}
	if second.DeadLettered != 1 {
		t.Fatalf("expected dead-lettered retry, got %+v", second)
	}

	dlq, err := broker.Poll(ctx, "inspectors", TopicDeadLetter, 10)
	if err != nil {
		t.Fatalf("poll dlq: %v", err)
	}
	if len(dlq) != 1 || dlq[0].LastError == "" {
		t.Fatalf("expected dlq message with error, got %+v", dlq)
	}
}

func lagFor(stats []TopicStats, key string) int64 {
	for _, stat := range stats {
		if value, ok := stat.Lag[key]; ok {
			return value
		}
	}
	return 0
}
