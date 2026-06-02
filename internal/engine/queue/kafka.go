package queue

import (
	"context"
	"encoding/json"
	"errors"
	"hash/crc32"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	Brokers      []string
	GroupID      string
	Partitions   int
	AutoCreate   bool
	WriteTimeout time.Duration
}

type KafkaBroker struct {
	cfg     KafkaConfig
	writer  *kafka.Writer
	mu      sync.Mutex
	readers map[string]*kafka.Reader
	pending map[string]map[int]map[int64]kafka.Message
	commits map[string]map[string]map[int]int64
}

func NewKafkaBroker(cfg KafkaConfig) *KafkaBroker {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"localhost:9092"}
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "logagg-writers"
	}
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	return &KafkaBroker{
		cfg: cfg,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(cfg.Brokers...),
			Balancer:               &kafka.CRC32Balancer{},
			AllowAutoTopicCreation: cfg.AutoCreate,
			BatchTimeout:           50 * time.Millisecond,
			RequiredAcks:           kafka.RequireAll,
		},
		readers: map[string]*kafka.Reader{},
		pending: map[string]map[int]map[int64]kafka.Message{},
		commits: map[string]map[string]map[int]int64{},
	}
}

func (b *KafkaBroker) Publish(ctx context.Context, req PublishRequest) (Message, error) {
	if req.Topic == "" {
		return Message{}, ErrTopicRequired
	}
	if err := ctx.Err(); err != nil {
		return Message{}, err
	}
	payload, err := json.Marshal(req.Event)
	if err != nil {
		return Message{}, err
	}
	writeCtx, cancel := context.WithTimeout(ctx, b.cfg.WriteTimeout)
	defer cancel()
	msg := kafka.Message{
		Topic: req.Topic,
		Key:   []byte(req.Key),
		Value: payload,
	}
	if err := b.writer.WriteMessages(writeCtx, msg); err != nil {
		return Message{}, err
	}
	partition := b.partition(req.Key)
	kafkaMsg := kafka.Message{
		Topic: req.Topic,
		Key:   []byte(req.Key),
		Value: payload,
	}
	return Message{
		Topic:     req.Topic,
		Partition: partition,
		Offset:    0,
		Key:       req.Key,
		Event:     req.Event.Clone(),
		Attempts:  req.Attempts,
		LastError: req.LastError,
		CreatedAt: time.Now().UTC(),
	}, b.rememberPending(req.Topic, partition, kafkaMsg)
}

func (b *KafkaBroker) Poll(ctx context.Context, group, topic string, limit int) ([]Message, error) {
	if topic == "" {
		return nil, ErrTopicRequired
	}
	if limit <= 0 {
		limit = 100
	}
	reader := b.reader(topic, group)
	out := make([]Message, 0, limit)
	for len(out) < limit {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return out, err
			}
			return out, err
		}
		var event Message
		if err := json.Unmarshal(msg.Value, &event); err == nil && event.Event.TenantID != "" {
			event.Topic = msg.Topic
			event.Partition = msg.Partition
			event.Offset = msg.Offset
			event.Key = string(msg.Key)
			event.CreatedAt = time.Now().UTC()
			out = append(out, event)
		} else {
			out = append(out, Message{
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Key:       string(msg.Key),
				Event:     event.Event,
				CreatedAt: time.Now().UTC(),
			})
		}
		b.rememberFetched(topic, msg)
	}
	return out, nil
}

func (b *KafkaBroker) Commit(ctx context.Context, group string, msg Message) error {
	reader := b.reader(msg.Topic, group)
	b.mu.Lock()
	stored := b.pending[msg.Topic][msg.Partition][msg.Offset]
	b.mu.Unlock()
	if stored.Topic == "" {
		stored = kafka.Message{
			Topic:     msg.Topic,
			Key:       []byte(msg.Key),
			Offset:    msg.Offset,
			Partition: msg.Partition,
		}
	}
	b.mu.Lock()
	if _, ok := b.commits[group]; !ok {
		b.commits[group] = map[string]map[int]int64{}
	}
	if _, ok := b.commits[group][msg.Topic]; !ok {
		b.commits[group][msg.Topic] = map[int]int64{}
	}
	b.commits[group][msg.Topic][msg.Partition] = msg.Offset + 1
	b.mu.Unlock()
	return reader.CommitMessages(ctx, stored)
}

func (b *KafkaBroker) Stats(ctx context.Context) []TopicStats {
	topics := []string{}
	for topic := range b.readers {
		topics = append(topics, topic)
	}
	if len(topics) == 0 {
		return nil
	}
	stats := make([]TopicStats, 0, len(topics))
	for _, topic := range topics {
		partitions := b.cfg.Partitions
		if partitions <= 0 {
			partitions = 1
		}
		highWatermark := map[int]int64{}
		for partition := 0; partition < partitions; partition++ {
			highWatermark[partition] = 0
		}
		stats = append(stats, TopicStats{
			Topic:         topic,
			Partitions:    partitions,
			HighWatermark: highWatermark,
			Committed:     b.committedSnapshot(topic),
			Lag:           b.lagSnapshot(topic),
		})
	}
	return stats
}

func (b *KafkaBroker) Replay(ctx context.Context, topic string, limit int) ([]Message, error) {
	if topic == "" {
		return nil, ErrTopicRequired
	}
	if limit <= 0 {
		limit = 100
	}
	out := make([]Message, 0, limit)
	for partition := 0; partition < b.cfg.Partitions && len(out) < limit; partition++ {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:       b.cfg.Brokers,
			Topic:         topic,
			Partition:     partition,
			StartOffset:   kafka.FirstOffset,
			MaxBytes:      10 << 20,
			QueueCapacity: 1,
		})
		for len(out) < limit {
			msg, err := reader.FetchMessage(ctx)
			if err != nil {
				break
			}
			var event Message
			if err := json.Unmarshal(msg.Value, &event); err == nil && event.Event.TenantID != "" {
				event.Topic = msg.Topic
				event.Partition = msg.Partition
				event.Offset = msg.Offset
				event.Key = string(msg.Key)
				event.CreatedAt = time.Now().UTC()
				out = append(out, event)
			} else {
				out = append(out, Message{
					Topic:     msg.Topic,
					Partition: msg.Partition,
					Offset:    msg.Offset,
					Key:       string(msg.Key),
					Event:     event.Event,
					CreatedAt: time.Now().UTC(),
				})
			}
		}
		_ = reader.Close()
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

func (b *KafkaBroker) reader(topic, group string) *kafka.Reader {
	b.mu.Lock()
	defer b.mu.Unlock()
	if reader, ok := b.readers[topic]; ok {
		return reader
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  b.cfg.Brokers,
		Topic:    topic,
		GroupID:  group,
		MaxBytes: 10 << 20,
	})
	b.readers[topic] = reader
	return reader
}

func (b *KafkaBroker) rememberFetched(topic string, msg kafka.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[topic]; !ok {
		b.pending[topic] = map[int]map[int64]kafka.Message{}
	}
	if _, ok := b.pending[topic][msg.Partition]; !ok {
		b.pending[topic][msg.Partition] = map[int64]kafka.Message{}
	}
	b.pending[topic][msg.Partition][msg.Offset] = msg
}

func (b *KafkaBroker) rememberPending(topic string, partition int, msg kafka.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.pending[topic]; !ok {
		b.pending[topic] = map[int]map[int64]kafka.Message{}
	}
	if _, ok := b.pending[topic][partition]; !ok {
		b.pending[topic][partition] = map[int64]kafka.Message{}
	}
	b.pending[topic][partition][0] = msg
	return nil
}

func (b *KafkaBroker) committedSnapshot(topic string) map[string]int64 {
	out := map[string]int64{}
	b.mu.Lock()
	defer b.mu.Unlock()
	for group, topics := range b.commits {
		for partition := 0; partition < b.cfg.Partitions; partition++ {
			out[group+":"+topic] += topics[topic][partition]
		}
	}
	return out
}

func (b *KafkaBroker) lagSnapshot(topic string) map[string]int64 {
	out := map[string]int64{}
	b.mu.Lock()
	defer b.mu.Unlock()
	for group := range b.commits {
		out[group+":"+topic] = 0
	}
	return out
}

func (b *KafkaBroker) partition(key string) int {
	if key == "" || b.cfg.Partitions == 1 {
		return 0
	}
	return int(crc32.ChecksumIEEE([]byte(key)) % uint32(b.cfg.Partitions))
}
