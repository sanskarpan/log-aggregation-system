package ring

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type State string

const (
	StateReady    State = "ready"
	StateDraining State = "draining"
	StateLeaving  State = "leaving"
)

type Member struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Zone      string    `json:"zone,omitempty"`
	State     State     `json:"state"`
	LeaseID   int64     `json:"lease_id,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Assignment struct {
	StreamKey   string   `json:"stream_key"`
	Fingerprint string   `json:"fingerprint"`
	Primary     Member   `json:"primary"`
	Replicas    []Member `json:"replicas"`
}

type TokenOwnership struct {
	Token    string   `json:"token"`
	Primary  Member   `json:"primary"`
	Replicas []Member `json:"replicas"`
}

type RebalanceReport struct {
	ReadyMembers     []Member              `json:"ready_members"`
	Tokens           []TokenOwnership      `json:"tokens"`
	PrimaryCount     map[string]int        `json:"primary_count"`
	ReplicaCount     map[string]int        `json:"replica_count"`
	AssignmentMatrix map[string]Assignment `json:"assignment_matrix,omitempty"`
}

type ReconcileReport struct {
	Upserted int      `json:"upserted"`
	Removed  int      `json:"removed"`
	Members  []Member `json:"members"`
}

func (m Member) Validate() error {
	if strings.TrimSpace(m.ID) == "" {
		return errors.New("member id is required")
	}
	if strings.TrimSpace(m.Address) == "" {
		return errors.New("member address is required")
	}
	switch m.State {
	case StateReady, StateDraining, StateLeaving, "":
	default:
		return fmt.Errorf("unsupported member state %q", m.State)
	}
	return nil
}

func dedupeMembers(members []Member) []Member {
	seen := map[string]struct{}{}
	out := make([]Member, 0, len(members))
	for _, member := range members {
		if _, ok := seen[member.ID]; ok {
			continue
		}
		seen[member.ID] = struct{}{}
		out = append(out, member)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].ID < out[j].ID
	})
	return out
}
