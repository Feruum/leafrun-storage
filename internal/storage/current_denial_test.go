package storage

import (
	"context"
	"errors"
	access "github.com/leafrun/leafrun-contracts/access/v1"
	identity "github.com/leafrun/leafrun-contracts/identity/v1"
	v1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"testing"
)

type publicCurrentDecision struct {
	allowed bool
	err     error
}

func (d publicCurrentDecision) Check(context.Context, access.CheckRequest) (bool, error) {
	return d.allowed, d.err
}
func TestPublicCurrentAccessDistinguishesDeniedAndUnavailable(t *testing.T) {
	id := identity.Identity{Principal: identity.Principal{Domain: "bot", Subject: "owned-bot"}, Authority: identity.AuthorityRef{Kind: identity.KindBotKey, ID: "key", Version: 1}}
	ctx, e := authn.WithIdentity(context.Background(), id)
	if e != nil {
		t.Fatal(e)
	}
	for _, tc := range []struct {
		name     string
		decision publicCurrentDecision
		status   int
	}{
		{"ceiling-denied", publicCurrentDecision{err: &access.HTTPError{Status: 403, Class: access.ErrDenied}}, 404},
		{"bound-authority-revoked", publicCurrentDecision{err: &access.HTTPError{Status: 401, Class: access.ErrUnauthenticated}}, 404},
		{"dependency-unavailable", publicCurrentDecision{err: &access.HTTPError{Status: 503, Class: access.ErrDependency}}, 503},
		{"transport-uncertain", publicCurrentDecision{err: errors.New("transport uncertainty")}, 503},
		{"relation-denied", publicCurrentDecision{}, 404},
		{"currently-allowed", publicCurrentDecision{allowed: true}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &Service{Config: Config{Access: tc.decision}}
			e := s.currentPublicAccess(ctx, id.Principal, v1.Scope{WorkspaceID: "w", ProjectID: "p"}, "artifact.read", "artifact")
			if tc.status == 0 {
				if e != nil {
					t.Fatal("current allowed access denied")
				}
				return
			}
			var result *fault
			if !errors.As(e, &result) {
				t.Fatal("public denial lacks bounded status")
			}
			if result.Status != tc.status {
				t.Fatalf("public access status=%d want=%d", result.Status, tc.status)
			}
		})
	}
}
