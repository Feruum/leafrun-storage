package storage

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"

	v "github.com/leafrun/leafrun-contracts/storage/v1"
)

type scopedReleaseOwner struct {
	*testOwner
	scope  v.Scope
	checks []v.AccessCheck
}

func (o *scopedReleaseOwner) Access(ctx context.Context, in v.AccessCheck) (bool, error) {
	o.checks = append(o.checks, in)
	if in.Scope != o.scope {
		return false, nil
	}
	return o.testOwner.Access(ctx, in)
}

func TestIntegrationBoundPinReleaseDistinguishesAuthorizedAbsenceFromDenial(t *testing.T) {
	s, _ := integration(t)
	ctx := context.Background()
	credential := s.Config.Credentials[0]
	p := v.Principal{Subject: credential.Subject, Domain: credential.Domain}
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, p, scope)
	pinValue, err := s.AcquirePin(ctx, p, v.AcquirePinRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderID: p.Subject, HolderDomain: p.Domain, RetentionPolicy: "ordinary", IdempotencyKey: id()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ActivatePin(ctx, p, pinValue.PinID); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	client, err := v.NewClient(server.URL, credential.Token, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	binding := v.PinReleaseRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderID: p.Subject, HolderDomain: p.Domain, RetentionPolicy: "ordinary"}
	owner := &scopedReleaseOwner{testOwner: s.Owner.(*testOwner), scope: scope}
	s.Owner = owner
	owner.deny = true
	_, err = client.ReleasePinBound(ctx, pinValue.PinID, binding)
	var failure *v.HTTPError
	if !errors.As(err, &failure) || failure.Status != 404 {
		t.Fatalf("access denial became absence: %v", err)
	}
	owner.deny = false
	foreign := binding
	foreign.Scope.ProjectID = id()
	_, err = client.ReleasePinBound(ctx, pinValue.PinID, foreign)
	if !errors.As(err, &failure) || failure.Status != 404 {
		t.Fatalf("foreign scope bypassed access: %v", err)
	}
	last := owner.checks[len(owner.checks)-1]
	if last.Scope != foreign.Scope || last.Operation != "pin.release" {
		t.Fatalf("requested scope was not authorized before lookup: %+v", last)
	}
	current, err := s.GetPin(ctx, p, pinValue.PinID)
	if err != nil || current.State != "active" {
		t.Fatalf("denied release changed pin: %+v, %v", current, err)
	}
	wrong := binding
	wrong.ObjectID = id()
	receipt, err := client.ReleasePinBound(ctx, pinValue.PinID, wrong)
	if err != nil || receipt.PinID != pinValue.PinID || receipt.Binding != wrong || receipt.State != v.PinReleaseAbsent {
		t.Fatalf("nonmatching binding revealed or changed pin: %+v, %v", receipt, err)
	}
	current, err = s.GetPin(ctx, p, pinValue.PinID)
	if err != nil || current.State != "active" {
		t.Fatalf("wrong binding released pin: %+v, %v", current, err)
	}
	wrong = binding
	wrong.HolderID = "different-holder"
	_, err = client.ReleasePinBound(ctx, pinValue.PinID, wrong)
	if !errors.As(err, &failure) || failure.Status != 403 {
		t.Fatalf("holder claim bypassed service identity: %v", err)
	}
	for range 2 {
		receipt, err = client.ReleasePinBound(ctx, pinValue.PinID, binding)
		if err != nil || receipt.PinID != pinValue.PinID || receipt.Binding != binding || receipt.State != v.PinReleaseReleased {
			t.Fatalf("matching binding failed idempotent release: %+v, %v", receipt, err)
		}
	}
	if _, err = s.DB.Pool.Exec(ctx, "DELETE FROM storage_pins WHERE id=$1", pinValue.PinID); err != nil {
		t.Fatal(err)
	}
	owner.deny = true
	_, err = client.ReleasePinBound(ctx, pinValue.PinID, binding)
	if !errors.As(err, &failure) || failure.Status != 404 {
		t.Fatalf("missing pin skipped current access: %v", err)
	}
	owner.deny = false
	receipt, err = client.ReleasePinBound(ctx, pinValue.PinID, binding)
	if err != nil || receipt.PinID != pinValue.PinID || receipt.Binding != binding || receipt.State != v.PinReleaseAbsent {
		t.Fatalf("authorized purged binding was not absent: %+v, %v", receipt, err)
	}
}
