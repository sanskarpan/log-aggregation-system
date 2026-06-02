package queue

import (
	"context"
	"errors"
	"hash/crc32"
	"sort"
	"sync"
	"time"
)

var ErrTopicRequired = errors.New("topic is required")

type MemoryBroker struct {
	mu         sync.RWMutex
	partitions int
	topics     map[string]map[int][]Message
	commits    map[string]map[string]map[int]int64
}

func NewMemoryBroker(partitions int) *MemoryBroker {
	if partitions <= 0 {
		partitions = 1
	}
	broker := &MemoryBroker{
		partitions: partitions,
		topics:     map[string]map[int][]Message{},
		commits:    map[string]map[string]map[int]int64{},
	}
	for _, topic := range []string{TopicNormalizedEvents, TopicRetryEvents, TopicDeadLetter} {
		broker.ensureTopicLocked(topic)
	}
	return broker
}

func (b *MemoryBroker) Publish(_ context.Context, req PublishRequest) (Message, error) {
	if req.Topic == "" {
		return Message{}, ErrTopicRequired
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.ensureTopicLocked(req.Topic)
	partition := b.partition(req.Key)
	offset := int64(len(b.topics[req.Topic][partition]))
	msg := Message{
		Topic:     req.Topic,
		Partition: partition,
		Offset:    offset,
		Key:       req.Key,
		Event:     req.Event.Clone(),
		Attempts:  req.Attempts,
		LastError: req.LastError,
		CreatedAt: time.Now().UTC(),
	}
	b.topics[req.Topic][partition] = append(b.topics[req.Topic][partition], msg)
	return msg, nil
}

func (b *MemoryBroker) Poll(_ context.Context, group, topic string, limit int) ([]Message, error) {
	if topic == "" {
		return nil, ErrTopicRequired
	}
	if limit <= 0 {
		limit = 100
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	partitions, ok := b.topics[topic]
	if !ok {
		return nil, nil
	}
	b.ensureGroupTopicLocked(group, topic)

	out := make([]Message, 0, limit)
	for partition := 0; partition < b.partitions && len(out) < limit; partition++ {
		committed := b.committedLocked(group, topic, partition)
		messages := partitions[partition]
		for offset := committed; offset < int64(len(messages)) && len(out) < limit; offset++ {
			out = append(out, messages[offset])
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].Offset < out[j].Offset
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (b *MemoryBroker) Commit(_ context.Context, group string, msg Message) error {
	if msg.Topic == "" {
		return ErrTopicRequired
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.ensureGroupTopicLocked(group, msg.Topic)
	nextOffset := msg.Offset + 1
	if b.commits[group][msg.Topic][msg.Partition] < nextOffset {
		b.commits[group][msg.Topic][msg.Partition] = nextOffset
	}
	return nil
}

func (b *MemoryBroker) Stats(_ context.Context) []TopicStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	topics := make([]string, 0, len(b.topics))
	for topic := range b.topics {
		topics = append(topics, topic)
	}
	sort.Strings(topics)

	stats := make([]TopicStats, 0, len(topics))
	for _, topic := range topics {
		topicStats := TopicStats{
			Topic:         topic,
			Partitions:    b.partitions,
			HighWatermark: map[int]int64{},
			Committed:     map[string]int64{},
			Lag:           map[string]int64{},
		}
		totalHighWatermark := int64(0)
		for partition := 0; partition < b.partitions; partition++ {
			highWatermark := int64(len(b.topics[topic][partition]))
			topicStats.HighWatermark[partition] = highWatermark
			totalHighWatermark += highWatermark
			topicStats.Messages += int(highWatermark)
		}
		for group, topics := range b.commits {
			var committed int64
			for partition := 0; partition < b.partitions; partition++ {
				committed += topics[topic][partition]
			}
			key := group + ":" + topic
			topicStats.Committed[key] = committed
			topicStats.Lag[key] = totalHighWatermark - committed
		}
		stats = append(stats, topicStats)
	}
	return stats
}

func (b *MemoryBroker) Replay(_ context.Context, topic string, limit int) ([]Message, error) {
	if topic == "" {
		return nil, ErrTopicRequired
	}
	if limit <= 0 {
		limit = 100
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	partitions, ok := b.topics[topic]
	if !ok {
		return nil, nil
	}

	out := make([]Message, 0, limit)
	for partition := 0; partition < b.partitions && len(out) < limit; partition++ {
		messages := partitions[partition]
		for _, msg := range messages {
			out = append(out, msg)
			if len(out) >= limit {
				break
			}
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			if out[i].Partition == out[j].Partition {
				return out[i].Offset < out[j].Offset
			}
			return out[i].Partition < out[j].Partition
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (b *MemoryBroker) ensureTopicLocked(topic string) {
	if _, ok := b.topics[topic]; ok {
		return
	}
	b.topics[topic] = map[int][]Message{}
	for partition := 0; partition < b.partitions; partition++ {
		b.topics[topic][partition] = []Message{}
	}
}

func (b *MemoryBroker) ensureGroupTopicLocked(group, topic string) {
	if _, ok := b.commits[group]; !ok {
		b.commits[group] = map[string]map[int]int64{}
	}
	if _, ok := b.commits[group][topic]; !ok {
		b.commits[group][topic] = map[int]int64{}
	}
	for partition := 0; partition < b.partitions; partition++ {
		if _, ok := b.commits[group][topic][partition]; !ok {
			b.commits[group][topic][partition] = 0
		}
	}
}

func (b *MemoryBroker) committedLocked(group, topic string, partition int) int64 {
	if _, ok := b.commits[group]; !ok {
		return 0
	}
	if _, ok := b.commits[group][topic]; !ok {
		return 0
	}
	return b.commits[group][topic][partition]
}

func (b *MemoryBroker) partition(key string) int {
	if key == "" || b.partitions == 1 {
		return 0
	}
	return int(crc32.ChecksumIEEE([]byte(key)) % uint32(b.partitions))
}
