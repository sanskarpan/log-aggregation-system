package ring

import (
	"context"
	"encoding/json"
	"testing"
)

func TestCoordinatorRegisterResolveAndSnapshot(t *testing.T) {
	coordinator := NewCoordinator(NewMemoryBackend(), "/ring", 2)
	ctx := context.Background()

	for _, member := range []Member{
		{ID: "a", Address: "10.0.0.1:9095", State: StateReady},
		{ID: "b", Address: "10.0.0.2:9095", State: StateReady},
		{ID: "c", Address: "10.0.0.3:9095", State: StateDraining},
	} {
		if _, err := coordinator.Register(ctx, member, 30); err != nil {
			t.Fatalf("register member: %v", err)
		}
	}

	assignment, err := coordinator.Resolve(ctx, "tenant-a|service=api|env=prod")
	if err != nil {
		t.Fatalf("resolve assignment: %v", err)
	}
	if assignment.Primary.ID == "" {
		t.Fatal("expected primary member")
	}
	if len(assignment.Replicas) != 1 {
		t.Fatalf("expected one replica, got %d", len(assignment.Replicas))
	}
	if assignment.Primary.ID == assignment.Replicas[0].ID {
		t.Fatal("expected distinct primary and replica")
	}

	snapshot, err := coordinator.Snapshot(ctx)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}
	if decoded["prefix"] != "/ring" {
		t.Fatalf("unexpected prefix: %v", decoded["prefix"])
	}
}

func TestCoordinatorStateTransitions(t *testing.T) {
	coordinator := NewCoordinator(NewMemoryBackend(), "/ring", 2)
	ctx := context.Background()

	if _, err := coordinator.Register(ctx, Member{ID: "a", Address: "10.0.0.1:9095"}, 30); err != nil {
		t.Fatalf("register member: %v", err)
	}
	updated, err := coordinator.SetState(ctx, "a", StateLeaving, 30)
	if err != nil {
		t.Fatalf("set state: %v", err)
	}
	if updated.State != StateLeaving {
		t.Fatalf("expected leaving state, got %s", updated.State)
	}
	if _, err := coordinator.Heartbeat(ctx, "a"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
}

func TestCoordinatorRebalanceAndReconcile(t *testing.T) {
	coordinator := NewCoordinator(NewMemoryBackend(), "/ring", 2)
	ctx := context.Background()

	for _, member := range []Member{
		{ID: "a", Address: "10.0.0.1:9095", State: StateReady},
		{ID: "b", Address: "10.0.0.2:9095", State: StateReady},
		{ID: "c", Address: "10.0.0.3:9095", State: StateReady},
	} {
		if _, err := coordinator.Register(ctx, member, 30); err != nil {
			t.Fatalf("register member: %v", err)
		}
	}

	report, err := coordinator.Rebalance(ctx, 12)
	if err != nil {
		t.Fatalf("rebalance: %v", err)
	}
	if len(report.Tokens) != 12 {
		t.Fatalf("expected 12 tokens, got %d", len(report.Tokens))
	}
	if len(report.PrimaryCount) != 3 {
		t.Fatalf("expected primary counts for 3 members, got %+v", report.PrimaryCount)
	}

	reconcile, err := coordinator.Reconcile(ctx, []Member{
		{ID: "b", Address: "10.0.0.2:9095", State: StateReady},
		{ID: "d", Address: "10.0.0.4:9095", State: StateReady},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if reconcile.Upserted != 2 {
		t.Fatalf("expected 2 upserted members, got %d", reconcile.Upserted)
	}
	if reconcile.Removed != 2 {
		t.Fatalf("expected 2 removed members, got %d", reconcile.Removed)
	}
	if len(reconcile.Members) != 2 {
		t.Fatalf("expected 2 members after reconcile, got %d", len(reconcile.Members))
	}
}
