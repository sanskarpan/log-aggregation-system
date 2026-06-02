package model

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// Event is the canonical log record shape used across the platform.
type Event struct {
	Timestamp     time.Time         `json:"timestamp"`
	TenantID      string            `json:"tenant_id"`
	StreamLabels  map[string]string `json:"stream_labels"`
	Body          string            `json:"body"`
	Severity      string            `json:"severity"`
	ResourceAttrs map[string]string `json:"resource_attrs"`
	LogAttrs      map[string]string `json:"log_attrs"`
	ParsedFields  map[string]string `json:"parsed_fields"`
	TraceID       string            `json:"trace_id"`
	SpanID        string            `json:"span_id"`
}

func (e Event) Validate() error {
	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}
	if strings.TrimSpace(e.TenantID) == "" {
		return errors.New("tenant_id is required")
	}
	if strings.TrimSpace(e.Body) == "" {
		return errors.New("body is required")
	}
	for key, value := range e.StreamLabels {
		if strings.TrimSpace(key) == "" {
			return errors.New("stream label key is required")
		}
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("stream label %q value is required", key)
		}
	}
	return nil
}

func (e Event) ValidateWithLimits(limits TenantLimits) error {
	if err := e.Validate(); err != nil {
		return err
	}
	if limits.MaxLabelsPerStream > 0 && len(e.StreamLabels) > limits.MaxLabelsPerStream {
		return fmt.Errorf("stream label count %d exceeds limit %d", len(e.StreamLabels), limits.MaxLabelsPerStream)
	}
	if limits.MaxBodyBytes > 0 && len(e.Body) > limits.MaxBodyBytes {
		return fmt.Errorf("body size %d exceeds limit %d", len(e.Body), limits.MaxBodyBytes)
	}
	if limits.MaxParsedFields > 0 && len(e.ParsedFields) > limits.MaxParsedFields {
		return fmt.Errorf("parsed field count %d exceeds limit %d", len(e.ParsedFields), limits.MaxParsedFields)
	}
	if limits.MaxFieldValueBytes > 0 {
		for key, value := range e.StreamLabels {
			if len(value) > limits.MaxFieldValueBytes {
				return fmt.Errorf("stream label %q value size %d exceeds limit %d", key, len(value), limits.MaxFieldValueBytes)
			}
		}
		for key, value := range e.ParsedFields {
			if len(value) > limits.MaxFieldValueBytes {
				return fmt.Errorf("parsed field %q value size %d exceeds limit %d", key, len(value), limits.MaxFieldValueBytes)
			}
		}
	}
	return nil
}

func (e Event) StreamKey() string {
	if len(e.StreamLabels) == 0 {
		return e.TenantID
	}

	keys := make([]string, 0, len(e.StreamLabels))
	for key := range e.StreamLabels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys)+1)
	parts = append(parts, e.TenantID)
	for _, key := range keys {
		parts = append(parts, key+"="+e.StreamLabels[key])
	}
	return strings.Join(parts, "|")
}

func (e Event) Clone() Event {
	return Event{
		Timestamp:     e.Timestamp,
		TenantID:      e.TenantID,
		StreamLabels:  cloneMap(e.StreamLabels),
		Body:          e.Body,
		Severity:      e.Severity,
		ResourceAttrs: cloneMap(e.ResourceAttrs),
		LogAttrs:      cloneMap(e.LogAttrs),
		ParsedFields:  cloneMap(e.ParsedFields),
		TraceID:       e.TraceID,
		SpanID:        e.SpanID,
	}
}

func cloneMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
