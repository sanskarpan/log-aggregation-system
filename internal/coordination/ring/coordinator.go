package ring

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

var ErrMemberNotFound = errors.New("member not found")

type Coordinator struct {
	backend           Backend
	prefix            string
	replicationFactor int
}

func NewCoordinator(backend Backend, prefix string, replicationFactor int) *Coordinator {
	if replicationFactor <= 0 {
		replicationFactor = 2
	}
	return &Coordinator{
		backend:           backend,
		prefix:            prefix,
		replicationFactor: replicationFactor,
	}
}

func (c *Coordinator) Register(ctx context.Context, member Member, ttl int64) (Member, error) {
	if err := member.Validate(); err != nil {
		return Member{}, err
	}
	member.State = normalizeState(member.State)
	if member.State == "" {
		member.State = StateReady
	}
	member.UpdatedAt = time.Now().UTC()
	stored, err := c.backend.PutMember(ctx, c.prefix, member, ttl)
	if err != nil {
		return Member{}, err
	}
	return stored, nil
}

func (c *Coordinator) SetState(ctx context.Context, memberID string, state State, ttl int64) (Member, error) {
	members, err := c.List(ctx)
	if err != nil {
		return Member{}, err
	}
	member, ok := findMember(members, memberID)
	if !ok {
		return Member{}, ErrMemberNotFound
	}
	member.State = normalizeState(state)
	member.UpdatedAt = time.Now().UTC()
	stored, err := c.backend.PutMember(ctx, c.prefix, member, ttl)
	if err != nil {
		return Member{}, err
	}
	return stored, nil
}

func (c *Coordinator) Heartbeat(ctx context.Context, memberID string) (Member, error) {
	members, err := c.List(ctx)
	if err != nil {
		return Member{}, err
	}
	member, ok := findMember(members, memberID)
	if !ok {
		return Member{}, ErrMemberNotFound
	}
	if err := c.backend.RefreshLease(ctx, member.LeaseID); err != nil {
		return Member{}, err
	}
	member.UpdatedAt = time.Now().UTC()
	stored, err := c.backend.PutMember(ctx, c.prefix, member, 30)
	if err != nil {
		return Member{}, err
	}
	return stored, nil
}

func (c *Coordinator) MarkDraining(ctx context.Context, memberID string, ttl int64) (Member, error) {
	return c.SetState(ctx, memberID, StateDraining, ttl)
}

func (c *Coordinator) MarkLeaving(ctx context.Context, memberID string, ttl int64) (Member, error) {
	return c.SetState(ctx, memberID, StateLeaving, ttl)
}

func (c *Coordinator) Delete(ctx context.Context, memberID string) error {
	members, err := c.List(ctx)
	if err != nil {
		return err
	}
	member, ok := findMember(members, memberID)
	if !ok {
		return ErrMemberNotFound
	}
	if err := c.backend.DeleteMember(ctx, c.prefix, memberID); err != nil {
		return err
	}
	if member.LeaseID != 0 {
		return c.backend.RevokeLease(ctx, member.LeaseID)
	}
	return nil
}

func (c *Coordinator) List(ctx context.Context) ([]Member, error) {
	members, err := c.backend.ListMembers(ctx, c.prefix)
	if err != nil {
		return nil, err
	}
	return dedupeMembers(members), nil
}

func (c *Coordinator) Resolve(ctx context.Context, streamKey string) (Assignment, error) {
	members, err := c.List(ctx)
	if err != nil {
		return Assignment{}, err
	}

	ready := make([]Member, 0, len(members))
	for _, member := range members {
		if normalizeState(member.State) == StateReady {
			ready = append(ready, member)
		}
	}
	if len(ready) == 0 {
		return Assignment{}, errors.New("no ready members available")
	}

	scored := scoreMembers(streamKey, ready)
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].score == scored[j].score {
			return scored[i].member.ID < scored[j].member.ID
		}
		return bytes.Compare(scored[i].score[:], scored[j].score[:]) > 0
	})

	selected := make([]Member, 0, c.replicationFactor)
	for _, item := range scored {
		selected = append(selected, item.member)
		if len(selected) >= c.replicationFactor {
			break
		}
	}

	fingerprint := fingerprint(streamKey)
	assignment := Assignment{
		StreamKey:   streamKey,
		Fingerprint: fingerprint,
		Primary:     selected[0],
	}
	if len(selected) > 1 {
		assignment.Replicas = append(assignment.Replicas, selected[1:]...)
	}
	return assignment, nil
}

func (c *Coordinator) Rebalance(ctx context.Context, tokenCount int) (RebalanceReport, error) {
	members, err := c.List(ctx)
	if err != nil {
		return RebalanceReport{}, err
	}
	ready := make([]Member, 0, len(members))
	for _, member := range members {
		if normalizeState(member.State) == StateReady {
			ready = append(ready, member)
		}
	}
	if len(ready) == 0 {
		return RebalanceReport{}, errors.New("no ready members available")
	}
	if tokenCount <= 0 {
		tokenCount = len(ready) * 32
	}

	primaryCount := map[string]int{}
	replicaCount := map[string]int{}
	matrix := map[string]Assignment{}
	own := make([]TokenOwnership, 0, tokenCount)
	for i := 0; i < tokenCount; i++ {
		token := fmt.Sprintf("%s/token/%d", c.prefix, i)
		assignment, err := c.Resolve(ctx, token)
		if err != nil {
			return RebalanceReport{}, err
		}
		primaryCount[assignment.Primary.ID]++
		for _, replica := range assignment.Replicas {
			replicaCount[replica.ID]++
		}
		matrix[token] = assignment
		own = append(own, TokenOwnership{
			Token:    token,
			Primary:  assignment.Primary,
			Replicas: assignment.Replicas,
		})
	}
	return RebalanceReport{
		ReadyMembers:     ready,
		Tokens:           own,
		PrimaryCount:     primaryCount,
		ReplicaCount:     replicaCount,
		AssignmentMatrix: matrix,
	}, nil
}

func (c *Coordinator) Reconcile(ctx context.Context, desired []Member) (ReconcileReport, error) {
	current, err := c.List(ctx)
	if err != nil {
		return ReconcileReport{}, err
	}
	currentByID := map[string]Member{}
	for _, member := range current {
		currentByID[member.ID] = member
	}
	desiredByID := map[string]Member{}
	for _, member := range desired {
		if err := member.Validate(); err != nil {
			return ReconcileReport{}, err
		}
		desiredByID[member.ID] = member
	}

	upserted := 0
	for _, member := range desired {
		existing, ok := currentByID[member.ID]
		if ok {
			member.LeaseID = existing.LeaseID
		}
		member.State = normalizeState(member.State)
		member.UpdatedAt = time.Now().UTC()
		if _, err := c.backend.PutMember(ctx, c.prefix, member, 30); err != nil {
			return ReconcileReport{}, err
		}
		upserted++
	}

	removed := 0
	for _, member := range current {
		if _, ok := desiredByID[member.ID]; ok {
			continue
		}
		if err := c.backend.DeleteMember(ctx, c.prefix, member.ID); err != nil {
			return ReconcileReport{}, err
		}
		if member.LeaseID != 0 {
			_ = c.backend.RevokeLease(ctx, member.LeaseID)
		}
		removed++
	}

	members, err := c.List(ctx)
	if err != nil {
		return ReconcileReport{}, err
	}
	return ReconcileReport{Upserted: upserted, Removed: removed, Members: members}, nil
}

func (c *Coordinator) Snapshot(ctx context.Context) ([]byte, error) {
	members, err := c.List(ctx)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(map[string]any{
		"prefix":             c.prefix,
		"replication_factor": c.replicationFactor,
		"members":            members,
	}, "", "  ")
}

type scoredMember struct {
	member Member
	score  [32]byte
}

func scoreMembers(streamKey string, members []Member) []scoredMember {
	out := make([]scoredMember, 0, len(members))
	for _, member := range members {
		sum := sha256.Sum256([]byte(streamKey + "|" + member.ID))
		out = append(out, scoredMember{member: member, score: sum})
	}
	return out
}

func fingerprint(streamKey string) string {
	sum := sha256.Sum256([]byte(streamKey))
	return hex.EncodeToString(sum[:])
}

func findMember(members []Member, memberID string) (Member, bool) {
	for _, member := range members {
		if member.ID == memberID {
			return member, true
		}
	}
	return Member{}, false
}

func normalizeState(state State) State {
	switch state {
	case StateReady, StateDraining, StateLeaving:
		return state
	case "":
		return StateReady
	default:
		return StateReady
	}
}
