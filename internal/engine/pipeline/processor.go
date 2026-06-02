package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

var ErrDropped = errors.New("event dropped by pipeline")

type Processor struct {
	mu    sync.Mutex
	stats model.ParseStats
}

func NewProcessor() *Processor {
	return &Processor{
		stats: model.ParseStats{StageErrors: map[string]int{}},
	}
}

func (p *Processor) Process(_ context.Context, tenant model.TenantConfig, event model.Event, pipeline model.ParsePipeline) (model.Event, error) {
	normalized := event.Clone()
	if normalized.StreamLabels == nil {
		normalized.StreamLabels = map[string]string{}
	}
	if normalized.ParsedFields == nil {
		normalized.ParsedFields = map[string]string{}
	}
	if normalized.ResourceAttrs == nil {
		normalized.ResourceAttrs = map[string]string{}
	}
	if normalized.LogAttrs == nil {
		normalized.LogAttrs = map[string]string{}
	}

	for _, stage := range pipeline.Stages {
		if err := p.applyStage(&normalized, stage); err != nil {
			p.recordStageError(stage.Name)
			if errors.Is(err, ErrDropped) {
				p.recordDropped()
				normalized.LogAttrs["_pipeline_drop"] = "true"
				return normalized, nil
			}
			if stage.Config["ignore_error"] == "true" {
				continue
			}
			return model.Event{}, fmt.Errorf("%s stage %q failed: %w", stage.Type, stage.Name, err)
		}
	}

	applyPromotionPolicy(&normalized, tenant)
	return normalized, normalized.Validate()
}

func (p *Processor) Stats() model.ParseStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := model.ParseStats{
		StageErrors:   map[string]int{},
		DroppedEvents: p.stats.DroppedEvents,
	}
	for key, value := range p.stats.StageErrors {
		out.StageErrors[key] = value
	}
	return out
}

func (p *Processor) applyStage(event *model.Event, stage model.PipelineStage) error {
	switch stage.Type {
	case "json":
		return applyJSONStage(event)
	case "logfmt":
		applyLogfmtStage(event)
		return nil
	case "regex":
		return applyRegexStage(event, stage.Config)
	case "static":
		applyStaticStage(event, stage.Config)
		return nil
	case "timestamp":
		return applyTimestampStage(event, stage.Config)
	case "severity":
		return applySeverityStage(event, stage.Config)
	case "filter":
		return applyFilterStage(event, stage.Config)
	case "rename":
		return applyRenameStage(event, stage.Config)
	case "redact":
		return applyRedactStage(event, stage.Config)
	default:
		return fmt.Errorf("unsupported stage type %q", stage.Type)
	}
}

func (p *Processor) recordStageError(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.StageErrors[name]++
}

func (p *Processor) recordDropped() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.DroppedEvents++
}

func applyJSONStage(event *model.Event) error {
	fields := map[string]any{}
	if err := json.Unmarshal([]byte(event.Body), &fields); err != nil {
		return err
	}

	for key, value := range fields {
		event.ParsedFields[key] = stringify(value)
	}
	return nil
}

func applyLogfmtStage(event *model.Event) {
	for _, token := range strings.Fields(event.Body) {
		key, value, ok := strings.Cut(token, "=")
		if !ok || key == "" {
			continue
		}
		event.ParsedFields[key] = strings.Trim(value, `"`)
	}
}

func applyRegexStage(event *model.Event, config map[string]string) error {
	pattern, ok := config["pattern"]
	if !ok || pattern == "" {
		return fmt.Errorf("pattern is required")
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return err
	}

	matches := re.FindStringSubmatch(event.Body)
	if matches == nil {
		return nil
	}

	for index, name := range re.SubexpNames() {
		if index == 0 || name == "" {
			continue
		}
		event.ParsedFields[name] = matches[index]
	}
	return nil
}

func applyStaticStage(event *model.Event, config map[string]string) {
	for key, value := range config {
		if strings.HasPrefix(key, "label.") {
			event.StreamLabels[strings.TrimPrefix(key, "label.")] = value
			continue
		}
		event.ParsedFields[key] = value
	}
}

func applyTimestampStage(event *model.Event, config map[string]string) error {
	source := config["source"]
	if source == "" {
		return errors.New("timestamp source is required")
	}
	value := event.ParsedFields[source]
	if value == "" {
		return nil
	}

	format := strings.ToLower(config["format"])
	switch format {
	case "", "rfc3339":
		ts, err := time.Parse(time.RFC3339, value)
		if err != nil {
			return err
		}
		event.Timestamp = ts.UTC()
		return nil
	case "unix", "unix_seconds":
		seconds, err := time.ParseDuration(value + "s")
		if err != nil {
			return err
		}
		event.Timestamp = time.Unix(int64(seconds.Seconds()), 0).UTC()
		return nil
	case "unix_nano":
		nanos, err := time.ParseDuration(value + "ns")
		if err != nil {
			return err
		}
		event.Timestamp = time.Unix(0, nanos.Nanoseconds()).UTC()
		return nil
	default:
		return fmt.Errorf("unsupported timestamp format %q", format)
	}
}

func applySeverityStage(event *model.Event, config map[string]string) error {
	value := config["value"]
	if value == "" {
		value = event.ParsedFields[config["source"]]
	}
	if value == "" {
		return nil
	}
	event.Severity = normalizeSeverity(value)
	return nil
}

func applyFilterStage(event *model.Event, config map[string]string) error {
	field := config["field"]
	if field == "" {
		return errors.New("filter field is required")
	}
	value := firstNonEmpty(event.ParsedFields[field], event.StreamLabels[field], event.ResourceAttrs[field], event.LogAttrs[field])
	expected := config["value"]
	op := strings.ToLower(firstNonEmpty(config["op"], "equals"))

	matched := false
	switch op {
	case "equals":
		matched = value == expected
	case "not_equals":
		matched = value != expected
	case "contains":
		matched = strings.Contains(value, expected)
	default:
		return fmt.Errorf("unsupported filter op %q", op)
	}
	if matched && strings.ToLower(firstNonEmpty(config["action"], "drop")) == "drop" {
		return ErrDropped
	}
	return nil
}

func applyRenameStage(event *model.Event, config map[string]string) error {
	from := config["from"]
	to := config["to"]
	if from == "" || to == "" {
		return errors.New("rename requires from and to")
	}
	target := strings.ToLower(firstNonEmpty(config["target"], "parsed"))
	switch target {
	case "parsed":
		value, ok := event.ParsedFields[from]
		if !ok {
			return nil
		}
		delete(event.ParsedFields, from)
		event.ParsedFields[to] = value
	case "label":
		value, ok := event.StreamLabels[from]
		if !ok {
			return nil
		}
		delete(event.StreamLabels, from)
		event.StreamLabels[to] = value
	default:
		return fmt.Errorf("unsupported rename target %q", target)
	}
	return nil
}

func applyRedactStage(event *model.Event, config map[string]string) error {
	field := config["field"]
	if field == "" {
		return errors.New("redact field is required")
	}
	replacement := firstNonEmpty(config["replacement"], "[REDACTED]")
	target := strings.ToLower(firstNonEmpty(config["target"], "parsed"))

	switch target {
	case "parsed":
		if _, ok := event.ParsedFields[field]; ok {
			event.ParsedFields[field] = replacement
		}
	case "body":
		if strings.Contains(event.Body, field) {
			event.Body = strings.ReplaceAll(event.Body, field, replacement)
		}
	default:
		return fmt.Errorf("unsupported redact target %q", target)
	}
	return nil
}

func applyPromotionPolicy(event *model.Event, tenant model.TenantConfig) {
	allow := make(map[string]struct{}, len(tenant.LabelPromotionAllow))
	for _, key := range tenant.LabelPromotionAllow {
		allow[key] = struct{}{}
	}

	deny := make(map[string]struct{}, len(tenant.LabelPromotionDeny))
	for _, key := range tenant.LabelPromotionDeny {
		deny[key] = struct{}{}
	}

	for _, key := range tenant.ReservedLabels {
		if value := firstNonEmpty(event.StreamLabels[key], event.ParsedFields[key], event.ResourceAttrs[key], event.LogAttrs[key]); value != "" {
			event.StreamLabels[key] = value
		}
	}

	for key, value := range event.ParsedFields {
		if _, denied := deny[key]; denied {
			continue
		}
		if _, allowed := allow[key]; allowed && event.StreamLabels[key] == "" {
			event.StreamLabels[key] = value
		}
	}

	if event.Severity != "" && event.StreamLabels["severity"] == "" {
		event.StreamLabels["severity"] = event.Severity
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func normalizeSeverity(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "warning":
		return "warn"
	case "trace", "debug", "info", "warn", "error", "fatal":
		return normalized
	default:
		return normalized
	}
}

func stringify(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.6f", typed), "0"), ".")
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", typed)
	}
}
