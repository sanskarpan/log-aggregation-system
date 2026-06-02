package distribution

import (
	"math"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type Admission struct {
	Allowed    bool
	RetryAfter time.Duration
	Reason     string
	UsedBytes  int64
	LimitBytes int64
}

type bucket struct {
	tokens     float64
	lastRefill time.Time
	rateBytes  float64
	burstBytes float64
}

type Limiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
}

func NewLimiter() *Limiter {
	return &Limiter{buckets: map[string]*bucket{}}
}

func (l *Limiter) Allow(tenant model.TenantConfig, event model.Event) Admission {
	usedBytes := eventSize(event)
	limitBytes := int64(tenant.Limits.MaxBodyBytes)
	if tenant.Limits.MaxLabelsPerStream > 0 {
		limitBytes += int64(len(event.StreamLabels)) * 128
	}

	if limitBytes > 0 && int64(usedBytes) > limitBytes {
		return Admission{
			Allowed:    false,
			Reason:     "event exceeds tenant body or label budget",
			UsedBytes:  int64(usedBytes),
			LimitBytes: limitBytes,
		}
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.bucketFor(tenant)
	now := time.Now().UTC()
	b.refill(now)
	if b.tokens < float64(usedBytes) {
		deficit := float64(usedBytes) - b.tokens
		retryAfter := time.Duration(math.Ceil(deficit/b.rateBytes*1000)) * time.Millisecond
		if retryAfter < time.Millisecond {
			retryAfter = time.Millisecond
		}
		return Admission{
			Allowed:    false,
			RetryAfter: retryAfter,
			Reason:     "tenant ingest rate exceeded",
			UsedBytes:  int64(usedBytes),
			LimitBytes: int64(b.burstBytes),
		}
	}

	b.tokens -= float64(usedBytes)
	return Admission{
		Allowed:    true,
		UsedBytes:  int64(usedBytes),
		LimitBytes: int64(b.burstBytes),
	}
}

func (l *Limiter) bucketFor(tenant model.TenantConfig) *bucket {
	if existing, ok := l.buckets[tenant.ID]; ok {
		return existing
	}

	rateBytes := float64(tenant.Limits.IngestRateMBPerSecond) * 1024 * 1024
	if rateBytes <= 0 {
		rateBytes = 1024 * 1024
	}
	burstBytes := rateBytes * 2
	b := &bucket{
		tokens:     burstBytes,
		lastRefill: time.Now().UTC(),
		rateBytes:  rateBytes,
		burstBytes: burstBytes,
	}
	l.buckets[tenant.ID] = b
	return b
}

func (b *bucket) refill(now time.Time) {
	elapsed := now.Sub(b.lastRefill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.rateBytes
	if b.tokens > b.burstBytes {
		b.tokens = b.burstBytes
	}
	b.lastRefill = now
}

func eventSize(event model.Event) int {
	total := len(event.Body) + len(event.TenantID) + len(event.Severity) + len(event.TraceID) + len(event.SpanID)
	for key, value := range event.StreamLabels {
		total += len(key) + len(value)
	}
	for key, value := range event.ResourceAttrs {
		total += len(key) + len(value)
	}
	for key, value := range event.LogAttrs {
		total += len(key) + len(value)
	}
	for key, value := range event.ParsedFields {
		total += len(key) + len(value)
	}
	return total
}
