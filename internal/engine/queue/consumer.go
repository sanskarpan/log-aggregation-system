package queue

import (
	"context"
	"errors"
)

type ConsumerOptions struct {
	Group       string
	Topics      []string
	MaxAttempts int
	BatchSize   int
}

type ConsumeReport struct {
	Polled       int `json:"polled"`
	Processed    int `json:"processed"`
	Retried      int `json:"retried"`
	DeadLettered int `json:"dead_lettered"`
}

type Consumer struct {
	broker  Broker
	handler Handler
	options ConsumerOptions
}

func NewConsumer(broker Broker, handler Handler, options ConsumerOptions) *Consumer {
	if options.Group == "" {
		options.Group = "logagg-writers"
	}
	if len(options.Topics) == 0 {
		options.Topics = []string{TopicNormalizedEvents, TopicRetryEvents}
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 3
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	return &Consumer{
		broker:  broker,
		handler: handler,
		options: options,
	}
}

func (c *Consumer) ConsumeOnce(ctx context.Context, limit int) (ConsumeReport, error) {
	if c.broker == nil {
		return ConsumeReport{}, errors.New("broker is required")
	}
	if c.handler == nil {
		return ConsumeReport{}, errors.New("handler is required")
	}
	if limit <= 0 {
		limit = c.options.BatchSize
	}

	report := ConsumeReport{}
	remaining := limit
	batch := make([]Message, 0, limit)
	for _, topic := range c.options.Topics {
		if remaining <= 0 {
			break
		}
		messages, err := c.broker.Poll(ctx, c.options.Group, topic, remaining)
		if err != nil {
			return report, err
		}
		report.Polled += len(messages)
		remaining -= len(messages)
		batch = append(batch, messages...)
	}

	for _, msg := range batch {
		if err := c.handler.Handle(ctx, msg); err != nil {
			if msg.Attempts+1 >= c.options.MaxAttempts {
				if _, publishErr := c.broker.Publish(ctx, PublishRequest{
					Topic:     TopicDeadLetter,
					Key:       msg.Key,
					Event:     msg.Event,
					Attempts:  msg.Attempts + 1,
					LastError: err.Error(),
				}); publishErr != nil {
					return report, publishErr
				}
				report.DeadLettered++
			} else {
				if _, publishErr := c.broker.Publish(ctx, PublishRequest{
					Topic:     TopicRetryEvents,
					Key:       msg.Key,
					Event:     msg.Event,
					Attempts:  msg.Attempts + 1,
					LastError: err.Error(),
				}); publishErr != nil {
					return report, publishErr
				}
				report.Retried++
			}
			if err := c.broker.Commit(ctx, c.options.Group, msg); err != nil {
				return report, err
			}
			continue
		}

		if err := c.broker.Commit(ctx, c.options.Group, msg); err != nil {
			return report, err
		}
		report.Processed++
	}
	return report, nil
}
