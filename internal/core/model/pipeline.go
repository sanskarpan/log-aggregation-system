package model

import "time"

type PipelineStage struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"`
	Config map[string]string `json:"config"`
}

type ParsePipeline struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	Stages      []PipelineStage `json:"stages"`
}

type ParseStats struct {
	StageErrors   map[string]int `json:"stage_errors"`
	DroppedEvents int            `json:"dropped_events"`
}

type TailRequest struct {
	TenantID        string            `json:"tenant_id"`
	LabelSelectors  map[string]string `json:"label_selectors"`
	FieldPredicates map[string]string `json:"field_predicates"`
	Limit           int               `json:"limit"`
	Since           time.Time         `json:"since"`
}
