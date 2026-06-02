package chunk

import (
	"bytes"
	"encoding/json"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type Metadata struct {
	EventCount       int               `json:"event_count"`
	ApproxBytes      int               `json:"approx_bytes"`
	StreamLabels     map[string]string `json:"stream_labels"`
	LabelKeys        []string          `json:"label_keys"`
	SearchableFields []string          `json:"searchable_fields"`
	BodyTokens       []string          `json:"body_tokens,omitempty"`
	BodyBloom        [4]uint64         `json:"body_bloom,omitempty"`
	MinTimestamp     time.Time         `json:"min_timestamp"`
	MaxTimestamp     time.Time         `json:"max_timestamp"`
}

type Chunk struct {
	StreamKey string        `json:"stream_key"`
	Start     time.Time     `json:"start"`
	End       time.Time     `json:"end"`
	ObjectKey string        `json:"object_key,omitempty"`
	Checksum  string        `json:"checksum,omitempty"`
	Metadata  Metadata      `json:"metadata"`
	Events    []model.Event `json:"events"`
}

type BuilderOptions struct {
	MaxEvents   int
	MaxBytes    int
	MaxDuration time.Duration
}

type Builder struct {
	streamKey string
	options   BuilderOptions
	events    []model.Event
	bytes     int
	start     time.Time
}

func NewBuilder(streamKey string, maxEvents int) *Builder {
	return NewBuilderWithOptions(streamKey, BuilderOptions{MaxEvents: maxEvents, MaxBytes: 256 * 1024, MaxDuration: time.Minute})
}

func NewBuilderWithOptions(streamKey string, options BuilderOptions) *Builder {
	if options.MaxEvents <= 0 {
		options.MaxEvents = 1000
	}
	if options.MaxBytes <= 0 {
		options.MaxBytes = 256 * 1024
	}
	if options.MaxDuration <= 0 {
		options.MaxDuration = time.Minute
	}
	return &Builder{
		streamKey: streamKey,
		options:   options,
		events:    make([]model.Event, 0, options.MaxEvents),
	}
}

func (b *Builder) Add(event model.Event) (bool, *Chunk) {
	if len(b.events) == 0 {
		b.start = event.Timestamp
	}
	b.events = append(b.events, event.Clone())
	b.bytes += estimateEventBytes(event)
	if !b.shouldFlush(event.Timestamp) {
		return false, nil
	}
	chunk := b.Flush()
	return true, chunk
}

func (b *Builder) Flush() *Chunk {
	if len(b.events) == 0 {
		return nil
	}
	chunk := &Chunk{
		StreamKey: b.streamKey,
		Start:     b.events[0].Timestamp,
		End:       b.events[len(b.events)-1].Timestamp,
		Metadata:  buildMetadata(b.events, b.bytes),
		Events:    append([]model.Event(nil), b.events...),
	}
	b.events = b.events[:0]
	b.bytes = 0
	b.start = time.Time{}
	return chunk
}

func (b *Builder) shouldFlush(ts time.Time) bool {
	return len(b.events) >= b.options.MaxEvents ||
		b.bytes >= b.options.MaxBytes ||
		(!b.start.IsZero() && ts.Sub(b.start) >= b.options.MaxDuration)
}

func Encode(chunk Chunk) ([]byte, error) {
	payload, err := json.Marshal(chunk)
	if err != nil {
		return nil, err
	}

	var buffer bytes.Buffer
	writer, err := zstd.NewWriter(&buffer)
	if err != nil {
		return nil, err
	}
	if _, err := writer.Write(payload); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func Decode(data []byte) (Chunk, error) {
	reader, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		return Chunk{}, err
	}
	defer reader.Close()

	var chunk Chunk
	if err := json.NewDecoder(reader).Decode(&chunk); err != nil {
		return Chunk{}, err
	}
	return chunk, nil
}

func estimateEventBytes(event model.Event) int {
	total := len(event.Body) + len(event.Severity) + len(event.TraceID) + len(event.SpanID) + len(event.TenantID)
	for key, value := range event.StreamLabels {
		total += len(key) + len(value)
	}
	for key, value := range event.ParsedFields {
		total += len(key) + len(value)
	}
	return total
}

func buildMetadata(events []model.Event, approxBytes int) Metadata {
	labelSet := map[string]struct{}{}
	fieldSet := map[string]struct{}{}
	tokenSet := map[string]struct{}{}
	streamLabels := map[string]string{}
	minTS := events[0].Timestamp
	maxTS := events[0].Timestamp
	var bloom [4]uint64

	for _, event := range events {
		if event.Timestamp.Before(minTS) {
			minTS = event.Timestamp
		}
		if event.Timestamp.After(maxTS) {
			maxTS = event.Timestamp
		}
		for key := range event.StreamLabels {
			labelSet[key] = struct{}{}
			if _, ok := streamLabels[key]; !ok {
				streamLabels[key] = event.StreamLabels[key]
			}
		}
		for key := range event.ParsedFields {
			fieldSet[key] = struct{}{}
		}
		for _, token := range tokenize(event.Body) {
			tokenSet[token] = struct{}{}
			addBloomToken(&bloom, token)
		}
	}

	labelKeys := mapKeys(labelSet)
	fieldKeys := mapKeys(fieldSet)
	sort.Strings(labelKeys)
	sort.Strings(fieldKeys)

	return Metadata{
		EventCount:       len(events),
		ApproxBytes:      approxBytes,
		StreamLabels:     streamLabels,
		LabelKeys:        labelKeys,
		SearchableFields: fieldKeys,
		BodyTokens:       mapKeys(tokenSet),
		BodyBloom:        bloom,
		MinTimestamp:     minTS,
		MaxTimestamp:     maxTS,
	}
}

func mapKeys(input map[string]struct{}) []string {
	out := make([]string, 0, len(input))
	for key := range input {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func tokenize(body string) []string {
	fields := strings.FieldsFunc(strings.ToLower(body), func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	})
	tokens := make([]string, 0, len(fields))
	for _, field := range fields {
		if field != "" {
			tokens = append(tokens, field)
		}
	}
	return tokens
}

func addBloomToken(bloom *[4]uint64, token string) {
	if token == "" {
		return
	}
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(token))
	hash := sum.Sum64()
	for i := 0; i < len(bloom); i++ {
		shift := uint(i * 16)
		bit := (hash >> shift) & 63
		bloom[i] |= 1 << bit
	}
}

func BloomContains(bloom [4]uint64, token string) bool {
	if token == "" {
		return true
	}
	sum := fnv.New64a()
	_, _ = sum.Write([]byte(token))
	hash := sum.Sum64()
	for i := 0; i < len(bloom); i++ {
		shift := uint(i * 16)
		bit := (hash >> shift) & 63
		if bloom[i]&(1<<bit) == 0 {
			return false
		}
	}
	return true
}
