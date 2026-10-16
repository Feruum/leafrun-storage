package storage

import (
	"context"
	"sync/atomic"
	"testing"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

type e06StorageCurrentAccess struct {
	revoked *atomic.Bool
	calls   int
}

func (a *e06StorageCurrentAccess) Check(_ context.Context, _ accessv1.CheckRequest) (bool, error) {
	a.calls++
	return !a.revoked.Load(), nil
}

type e06StoragePublicationOwner struct {
	Owner
	revoked *atomic.Bool
	revoke  bool
}

func (o e06StoragePublicationOwner) Publishable(_ context.Context, _ storagev1.PublishabilityCheck) (bool, error) {
	if o.revoke {
		o.revoked.Store(true)
	}
	return true, nil
}

// Controlled dependencies isolate current reader versus publication provider
// ordering. No blob, database or actual Ory behavior is claimed here.
func TestE06StorageCurrentDownloadAfterPublicationProvider(t *testing.T) {
	for _, revoke := range []bool{true, false} {
		name := "fresh-positive-control"
		if revoke {
			name = "revoked-during-publication-provider"
		}
		t.Run(name, func(t *testing.T) {
			var revoked atomic.Bool
			id := identityv1.Identity{Principal: identityv1.Principal{Subject: "reader", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "reader-session", Version: 1}}
			ctx, err := authn.WithIdentity(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			access := &e06StorageCurrentAccess{revoked: &revoked}
			service := &Service{Config: Config{AuthMode: "access", Access: access}, Owner: e06StoragePublicationOwner{revoked: &revoked, revoke: revoke}}
			err = service.publication(ctx, id.Principal, storagev1.Artifact{ArtifactID: "artifact", Scope: storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}, OriginJobID: "job"})
			if revoke && err == nil {
				t.Fatalf("current reader gate used stale allow across publication provider; Access calls=%d", access.calls)
			}
			if !revoke && err != nil {
				t.Fatalf("fresh authorized publication denied: %v", err)
			}
		})
	}
}
