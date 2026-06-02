package distribution

import (
	"time"

	"github.com/sanskar/log-aggregation-system/internal/coordination/ring"
	"github.com/sanskar/log-aggregation-system/internal/core/model"
)

type AckStatus string

const (
	AckStatusAcked   AckStatus = "acked"
	AckStatusMissing AckStatus = "missing"
)

type Acknowledgment struct {
	RequiredAcks   int           `json:"required_acks"`
	AvailableAcks  int           `json:"available_acks"`
	AckedMembers   []ring.Member `json:"acked_members"`
	MissingMembers []ring.Member `json:"missing_members"`
	PartialSuccess bool          `json:"partial_success"`
	Strict         bool          `json:"strict"`
	Status         string        `json:"status"`
}

type RouteDecision struct {
	Event          model.Event     `json:"event"`
	Assignment     ring.Assignment `json:"assignment"`
	Acknowledgment Acknowledgment  `json:"acknowledgment"`
	Backpressure   *Backpressure   `json:"backpressure,omitempty"`
}

type Backpressure struct {
	Allowed    bool          `json:"allowed"`
	Reason     string        `json:"reason"`
	RetryAfter time.Duration `json:"retry_after"`
	UsedBytes  int64         `json:"used_bytes"`
	LimitBytes int64         `json:"limit_bytes"`
}
