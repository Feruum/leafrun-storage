package storage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	v "github.com/leafrun/leafrun-contracts/storage/v1"
)

func TestOwnerRejectsAmbiguousDecisionResponses(t *testing.T) {
	checks := []struct {
		name, valid, duplicate, wrongCase string
		call                              func(*HTTPOwner) error
	}{
		{"access", `{"allowed":true}`, `{"allowed":false,"allowed":true}`, `{"Allowed":true}`, func(owner *HTTPOwner) error {
			_, err := owner.Access(context.Background(), v.AccessCheck{})
			return err
		}},
		{"publication", `{"publishable":true}`, `{"publishable":false,"publishable":true}`, `{"Publishable":true}`, func(owner *HTTPOwner) error {
			_, err := owner.Publishable(context.Background(), v.PublishabilityCheck{})
			return err
		}},
		{"pin-owner", `{"state":"absent"}`, `{"state":"unknown","state":"absent"}`, `{"State":"absent"}`, func(owner *HTTPOwner) error {
			_, err := owner.Pin(context.Background(), v.PinOwnerCheck{})
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			for _, response := range []struct{ name, body string }{
				{"trailing-value", check.valid + `{}`},
				{"trailing-garbage", check.valid + `broken`},
				{"duplicate-decision", check.duplicate},
				{"wrong-field-case", check.wrongCase},
				{"oversized-response", check.valid + strings.Repeat(" ", 1<<20)},
			} {
				t.Run(response.name, func(t *testing.T) {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						_, _ = io.WriteString(w, response.body)
					}))
					defer server.Close()
					if err := check.call(NewOwner(server.URL, "owner-test-token")); err == nil {
						t.Fatal("ambiguous dependency response was accepted as an authoritative decision")
					}
				})
			}
			t.Run("complete-valid-response", func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					_, _ = io.WriteString(w, " \n"+check.valid+"\n ")
				}))
				defer server.Close()
				if err := check.call(NewOwner(server.URL, "owner-test-token")); err != nil {
					t.Fatalf("valid dependency response rejected: %v", err)
				}
			})
		})
	}
}

func TestIntegrationOwnerMalformedAbsencePreservesSnapshot(t *testing.T) {
	s, principal := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, principal, scope)
	pending, err := s.AcquirePin(context.Background(), principal, v.AcquirePinRequest{
		Scope: scope, ObjectID: snapshot.SnapshotID, HolderID: principal.Subject,
		HolderDomain: principal.Domain, RetentionPolicy: "release", IdempotencyKey: id(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Pool.Exec(context.Background(), "UPDATE storage_pins SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", pending.PinID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '40 days' WHERE id=$1", snapshot.SnapshotID); err != nil {
		t.Fatal(err)
	}
	ownerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/v1/checks/access" {
			_, _ = io.WriteString(w, `{"allowed":true}`)
			return
		}
		_, _ = io.WriteString(w, `{"state":"absent"}{"state":"referenced"}`)
	}))
	defer ownerServer.Close()
	s.Owner = NewOwner(ownerServer.URL, "owner-test-token")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = s.GC(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetSnapshot(ctx, principal, snapshot.SnapshotID); err != nil {
		t.Fatalf("malformed absence decision deleted the protected snapshot: %v", err)
	}
	current, err := s.GetPin(ctx, principal, pending.PinID)
	if err != nil || current.State != "unresolved" {
		t.Fatalf("uncertain owner pin state = %q, err = %v; want unresolved", current.State, err)
	}
}
