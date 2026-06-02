package model

import (
	"errors"
	"strconv"
	"time"
)

type QueryRequest struct {
	TenantID        string            `json:"tenant_id"`
	LabelSelectors  map[string]string `json:"label_selectors"`
	FieldPredicates map[string]string `json:"field_predicates"`
	TextContains    string            `json:"text_contains"`
	TextRegex       string            `json:"text_regex"`
	Start           time.Time         `json:"start"`
	End             time.Time         `json:"end"`
	Limit           int               `json:"limit"`
	Cursor          string            `json:"cursor,omitempty"`
	Tail            bool              `json:"tail,omitempty"`
	PartitionKey    string            `json:"partition_key,omitempty"`
}

type QueryResult struct {
	Events     []Event `json:"events"`
	Scanned    int64   `json:"scanned"`
	Matched    int64   `json:"matched"`
	Partial    bool    `json:"partial"`
	NextCursor string  `json:"next_cursor,omitempty"`
	Stats      Stats   `json:"stats"`
}

type QueryPlan struct {
	Request          QueryRequest    `json:"request"`
	Fragments        []QueryFragment `json:"fragments,omitempty"`
	Nodes            []PlanNode      `json:"nodes"`
	CandidateStreams int             `json:"candidate_streams"`
	CandidateChunks  int             `json:"candidate_chunks"`
	EstimatedCost    int             `json:"estimated_cost"`
	Partial          bool            `json:"partial"`
}

type QueryFragment struct {
	PartitionKey string `json:"partition_key"`
	ChunkCount   int    `json:"chunk_count"`
}

type PlanNode struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	Inputs      int    `json:"inputs"`
	Outputs     int    `json:"outputs"`
}

type Stats struct {
	CandidateStreams int `json:"candidate_streams"`
	CandidateChunks  int `json:"candidate_chunks"`
	ScannedEvents    int `json:"scanned_events"`
}

func (q QueryRequest) Validate() error {
	if q.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if q.Start.IsZero() || q.End.IsZero() {
		return errors.New("start and end are required")
	}
	if q.End.Before(q.Start) {
		return errors.New("end must be after start")
	}
	if q.Limit < 0 {
		return errors.New("limit must be non-negative")
	}
	return nil
}

func (q QueryRequest) CursorOffset() (int, error) {
	if q.Cursor == "" {
		return 0, nil
	}
	offset, err := strconv.Atoi(q.Cursor)
	if err != nil || offset < 0 {
		return 0, errors.New("cursor must be a non-negative integer offset")
	}
	return offset, nil
}
