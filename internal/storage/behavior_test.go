package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPRejectsSpoofedPrincipal(t *testing.T) {
	s := &Service{Config: Config{Credentials: []Credential{{Token: "secret-token-123456", Subject: "core", Domain: "core"}}}}
	r := httptest.NewRequest("POST", "/internal/v1/uploads", strings.NewReader(`{}`))
	r.Header.Set("X-Principal", "core")
	r.Header.Set("X-Scope", "w/p")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestOwnerHTTPFailsClosedAndAuthenticates(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer owner-secret" {
			t.Fatal("owner bearer absent")
		}
		if r.URL.Path != "/internal/v1/checks/access" {
			t.Fatal(r.URL.Path)
		}
		io.WriteString(w, `{"allowed":true}`)
	}))
	defer server.Close()
	o := NewOwner(server.URL, "owner-secret")
	ok, e := o.Access(context.Background(), v.AccessCheck{})
	if e != nil || !ok {
		t.Fatal(e)
	}
	server.Close()
	if _, e = o.Access(context.Background(), v.AccessCheck{}); e == nil {
		t.Fatal("outage allowed access")
	}
}
func TestReadLeaseEOFCancel(t *testing.T) {
	for _, cancelFirst := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		count := 0
		r := &leasedReader{body: io.NopCloser(strings.NewReader("content")), ctx: ctx, cancel: cancel, release: func() { count++ }}
		if cancelFirst {
			cancel()
		}
		_, e := io.ReadAll(r)
		if cancelFirst && e == nil {
			t.Fatal("cancel ignored")
		}
		r.Close()
		r.Close()
		if count != 1 {
			t.Fatal("release was not exactly once", count)
		}
	}
}
func TestIntegrationPendingUnknownAndExpiredActivation(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, p, scope)
	pin, e := s.AcquirePin(context.Background(), p, v.AcquirePinRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderID: p.Subject, HolderDomain: p.Domain, RetentionPolicy: "release", IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_pins SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", pin.PinID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '40 days' WHERE id=$1", snapshot.SnapshotID)
	if e != nil {
		t.Fatal(e)
	}
	s.Owner.(*testOwner).state = v.OwnerUnknown
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.GetSnapshot(context.Background(), p, snapshot.SnapshotID); e != nil {
		t.Fatal("unknown owner dropped reference", e)
	}
	s.Owner.(*testOwner).state = v.OwnerReferenced
	_, e = s.ActivatePin(context.Background(), p, pin.PinID)
	wantStatus(t, e, 409)
	s.Owner.(*testOwner).state = v.OwnerAbsent
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	_, e = s.GetSnapshot(context.Background(), p, snapshot.SnapshotID)
	wantStatus(t, e, 409)
}
func TestIntegrationRecoveryOriginalDeadline(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	u := uploaded(t, s, p, scope, v.ClassRecovery, "recovery")
	r, e := s.Commit(context.Background(), p, u.UploadID, v.CommitResourceRequest{WriterToken: u.WriterToken, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	o, e := getObject(context.Background(), s.DB.Pool, r.ResourceID)
	if e != nil || o.Expires == nil || o.Expires.Sub(o.Created) != 30*24*time.Hour {
		t.Fatal("recovery deadline reset", e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", r.ResourceID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.AcquirePin(context.Background(), p, v.AcquirePinRequest{Scope: scope, ObjectID: r.ResourceID, HolderID: p.Subject, HolderDomain: p.Domain, RetentionPolicy: "recovery", IdempotencyKey: id()})
	wantStatus(t, e, 410)
}
