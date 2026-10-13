package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"testing"
	"time"
)

type fairnessOwner struct {
	testOwner
	absentID string
}

func (o *fairnessOwner) Pin(_ context.Context, x v.PinOwnerCheck) (v.OwnerState, error) {
	if x.PinID == o.absentID {
		return v.OwnerAbsent, nil
	}
	return v.OwnerUnknown, nil
}
func TestIntegrationPinReconciliationFairness(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, p, scope)
	ctx := context.Background()
	unknownIDs := []string{}
	for i := 0; i < 105; i++ {
		pin, e := s.AcquirePin(ctx, p, v.AcquirePinRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderDomain: p.Domain, HolderID: p.Subject, RetentionPolicy: "release", IdempotencyKey: id()})
		if e != nil {
			t.Fatal(e)
		}
		unknownIDs = append(unknownIDs, pin.PinID)
		if _, e = s.DB.Pool.Exec(ctx, "UPDATE storage_pins SET expires_at=clock_timestamp()-interval '2 days'+$2*interval '1 second' WHERE id=$1", pin.PinID, i); e != nil {
			t.Fatal(e)
		}
	}
	absent, e := s.AcquirePin(ctx, p, v.AcquirePinRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderDomain: p.Domain, HolderID: p.Subject, RetentionPolicy: "release", IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB.Pool.Exec(ctx, "UPDATE storage_pins SET expires_at=clock_timestamp()-interval '1 day' WHERE id=$1", absent.PinID); e != nil {
		t.Fatal(e)
	}
	s.Owner = &fairnessOwner{testOwner: testOwner{publish: true}, absentID: absent.PinID}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = s.DB.Pool.Exec(cleanup, "DELETE FROM storage_pins WHERE object_id=$1", snapshot.SnapshotID)
	})
	for range 2 {
		if e = s.reconcilePins(ctx); e != nil {
			t.Fatal(e)
		}
	}
	receipt, e := s.GetPin(ctx, p, absent.PinID)
	if e != nil || receipt.State != "released" {
		t.Fatalf("later absent owner starved behind unknown batch: %q %v", receipt.State, e)
	}
	var protected int
	if e = s.DB.Pool.QueryRow(ctx, "SELECT count(*) FROM storage_pins WHERE object_id=$1 AND state IN ('pending','unresolved')", snapshot.SnapshotID).Scan(&protected); e != nil || protected != 105 {
		t.Fatalf("unknown references lost: %d %v", protected, e)
	}
	var attempted int
	if e = s.DB.Pool.QueryRow(ctx, "SELECT count(*) FROM storage_pins WHERE object_id=$1 AND last_reconciled_at IS NOT NULL", snapshot.SnapshotID).Scan(&attempted); e != nil || attempted != 106 {
		t.Fatalf("fairness timestamp not durable: %d %v", attempted, e)
	}
	if e = s.DB.Migrate(ctx); e != nil {
		t.Fatal("idempotent migration over populated old stand", e)
	}
	receipt, e = s.GetPin(ctx, p, absent.PinID)
	if e != nil || receipt.State != "released" {
		t.Fatal("migration lost existing pin state", e)
	}
}
