package model

import (
	"errors"
	"strings"
	"time"
)

type TenantLimits struct {
	IngestRateMBPerSecond int           `json:"ingest_rate_mb_per_second"`
	QueryConcurrency      int           `json:"query_concurrency"`
	MaxLabelsPerStream    int           `json:"max_labels_per_stream"`
	MaxBodyBytes          int           `json:"max_body_bytes"`
	MaxParsedFields       int           `json:"max_parsed_fields"`
	MaxFieldValueBytes    int           `json:"max_field_value_bytes"`
	RetentionClass        string        `json:"retention_class"`
	Retention             time.Duration `json:"retention"`
}

type TenantConfig struct {
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Version               int64             `json:"version"`
	UpdatedAt             time.Time         `json:"updated_at"`
	Protected             bool              `json:"protected"`
	LegalHoldUntil        time.Time         `json:"legal_hold_until"`
	SearchableFields      []string          `json:"searchable_fields"`
	ReservedLabels        []string          `json:"reserved_labels"`
	LabelPromotionAllow   []string          `json:"label_promotion_allow"`
	LabelPromotionDeny    []string          `json:"label_promotion_deny"`
	StructuredFieldPolicy map[string]string `json:"structured_field_policy"`
	Pipelines             []ParsePipeline   `json:"pipelines"`
	ActivePipelineID      string            `json:"active_pipeline_id"`
	Limits                TenantLimits      `json:"limits"`
}

func (t TenantConfig) Validate() error {
	if strings.TrimSpace(t.ID) == "" {
		return errors.New("tenant id is required")
	}
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("tenant name is required")
	}
	if t.Limits.MaxLabelsPerStream <= 0 {
		return errors.New("max_labels_per_stream must be positive")
	}
	if t.Limits.MaxBodyBytes <= 0 {
		return errors.New("max_body_bytes must be positive")
	}
	if t.Limits.MaxParsedFields <= 0 {
		return errors.New("max_parsed_fields must be positive")
	}
	if t.Limits.MaxFieldValueBytes <= 0 {
		return errors.New("max_field_value_bytes must be positive")
	}
	if t.Limits.Retention <= 0 {
		return errors.New("retention must be positive")
	}
	return nil
}
