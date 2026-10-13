package storage

import (
	"context"
	"errors"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type testOwner struct {
	deny, publish bool
	state         v.OwnerState
	err           error
}

func (o *testOwner) Access(context.Context, v.AccessCheck) (bool, error) { return !o.deny, o.err }
func (o *testOwner) Publishable(context.Context, v.PublishabilityCheck) (bool, error) {
	return o.publish, o.err
}
func (o *testOwner) Pin(context.Context, v.PinOwnerCheck) (v.OwnerState, error) {
	return o.state, o.err
}
func integration(t *testing.T) (*Service, v.Principal) {
	t.Helper()
	if os.Getenv("LEAFRUN_STORAGE_INTEGRATION") != "1" {
		t.Skip("set LEAFRUN_STORAGE_INTEGRATION=1 with dedicated PostgreSQL and private S3")
	}
	c, e := LoadConfig()
	if e != nil {
		t.Fatal(e)
	}
	d, e := OpenDB(context.Background(), c.DatabaseURL)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(d.Pool.Close)
	if e = d.Migrate(context.Background()); e != nil {
		t.Fatal(e)
	}
	b, e := OpenS3(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.Probe(context.Background()); e != nil {
		t.Fatal(e)
	}
	return &Service{DB: d, Blob: b, Owner: &testOwner{publish: true, state: v.OwnerReferenced}, Config: c}, v.Principal{Subject: "test-" + id(), Domain: "core"}
}
func uploaded(t *testing.T, s *Service, p v.Principal, scope v.Scope, class v.ObjectClass, body string) v.Upload {
	t.Helper()
	in := v.CreateUploadRequest{Scope: scope, Class: class, SHA256: fingerprintBytes(body), SizeBytes: int64(len(body)), IdempotencyKey: id()}
	u, e := s.CreateUpload(context.Background(), p, in)
	if e != nil {
		t.Fatal(e)
	}
	u, e = s.Put(context.Background(), p, u.UploadID, u.WriterToken, strings.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	return u
}
func committed(t *testing.T, s *Service, p v.Principal, scope v.Scope, body string) v.Resource {
	t.Helper()
	u := uploaded(t, s, p, scope, v.ClassResource, body)
	r, e := s.Commit(context.Background(), p, u.UploadID, v.CommitResourceRequest{WriterToken: u.WriterToken, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func snap(t *testing.T, s *Service, p v.Principal, scope v.Scope) v.Snapshot {
	t.Helper()
	r := committed(t, s, p, scope, "Hello")
	m := v.SnapshotManifest{FormatVersion: 1, Files: []v.SnapshotFile{{Path: "main.typ", SHA256: r.SHA256, Size: r.SizeBytes}}, Directories: []string{}, Fonts: []v.SnapshotFont{}}
	m.ID, _ = v.ManifestContentID(m)
	x, e := s.CreateSnapshot(context.Background(), p, v.CreateSnapshotRequest{Scope: scope, Manifest: m, Resources: []v.ResourceRef{{Tree: "project", Path: "main.typ", ResourceID: r.ResourceID}}, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	return x
}
func wantStatus(t *testing.T, e error, status int) {
	t.Helper()
	var f *fault
	if !errors.As(e, &f) || f.Status != status {
		t.Fatalf("want status %d, got %v", status, e)
	}
}
func TestIntegrationScopedDedupAndRead(t *testing.T) {
	s, p := integration(t)
	a := v.Scope{WorkspaceID: id(), ProjectID: id()}
	b := v.Scope{WorkspaceID: a.WorkspaceID, ProjectID: id()}
	one := committed(t, s, p, a, "same bytes")
	two := committed(t, s, p, a, "same bytes")
	three := committed(t, s, p, b, "same bytes")
	if one.ResourceID != two.ResourceID || one.ResourceID == three.ResourceID {
		t.Fatal("dedup violated scope boundary")
	}
	body, _, _, e := s.OpenRead(context.Background(), p, one.ResourceID, false)
	if e != nil {
		t.Fatal(e)
	}
	got, e := io.ReadAll(body)
	if e != nil || string(got) != "same bytes" {
		t.Fatal(string(got), e)
	}
	var leases int
	e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_read_leases WHERE object_id=$1", one.ResourceID).Scan(&leases)
	if e != nil || leases != 0 {
		t.Fatal("EOF did not release lease", e)
	}
	s.Owner.(*testOwner).deny = true
	_, _, _, e = s.OpenRead(context.Background(), p, one.ResourceID, false)
	wantStatus(t, e, 404)
}
func TestIntegrationFinalizeReceiptConcurrencyExpiry(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, p, scope)
	u := uploaded(t, s, p, scope, v.ClassResult, "%PDF-1.7")
	in := v.FinalizeArtifactRequest{WriterToken: u.WriterToken, SnapshotID: snapshot.SnapshotID, OriginJobID: id(), Metadata: v.ArtifactMetadata{Filename: "a.pdf", MediaType: "application/pdf", TargetID: "target", AttemptID: "attempt"}, IdempotencyKey: id()}
	results := make(chan v.Artifact, 8)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a, e := s.Finalize(context.Background(), p, u.UploadID, in)
			results <- a
			errs <- e
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var first v.Artifact
	for a := range results {
		if first.ArtifactID == "" {
			first = a
		}
		if a.ArtifactID != first.ArtifactID || !a.CreatedAt.Equal(first.CreatedAt) || a.ExpiresAt.Sub(a.CreatedAt) != 24*time.Hour {
			t.Fatal("immutable finalize receipt violated")
		}
	}
	_, e := s.DB.Pool.Exec(context.Background(), "UPDATE storage_uploads SET lease_until=clock_timestamp()-interval '1 minute' WHERE id=$1", u.UploadID)
	if e != nil {
		t.Fatal(e)
	}
	retry, e := s.Finalize(context.Background(), p, u.UploadID, in)
	if e != nil || retry != first {
		t.Fatal("expired writer receipt recovery", e)
	}
	changed := in
	changed.Metadata.Filename = "changed.pdf"
	_, e = s.Finalize(context.Background(), p, u.UploadID, changed)
	wantStatus(t, e, 409)
	s.Owner.(*testOwner).publish = false
	_, e = s.GetArtifact(context.Background(), p, first.ArtifactID)
	wantStatus(t, e, 404)
	s.Owner.(*testOwner).publish = true
	pin, e := s.AcquirePin(context.Background(), p, v.AcquirePinRequest{Scope: scope, ObjectID: first.ArtifactID, HolderDomain: p.Domain, HolderID: p.Subject, RetentionPolicy: "ordinary", IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.ActivatePin(context.Background(), p, pin.PinID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", first.ArtifactID)
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.GetArtifact(context.Background(), p, first.ArtifactID)
	wantStatus(t, e, 410)
	_, _, _, e = s.OpenRead(context.Background(), p, first.ArtifactID, true)
	wantStatus(t, e, 410)
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	o, e := getObject(context.Background(), s.DB.Pool, first.ArtifactID)
	if e != nil || o.State != "deleted" {
		t.Fatal("ordinary pin retained expired result", o.State, e)
	}
}
func TestIntegrationSnapshotBindingsAndReleaseRetention(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	snapshot := snap(t, s, p, scope)
	in := v.AcquirePinRequest{Scope: scope, ObjectID: snapshot.SnapshotID, HolderID: p.Subject, HolderDomain: p.Domain, RetentionPolicy: "release", IdempotencyKey: id()}
	pin, e := s.AcquirePin(context.Background(), p, in)
	if e != nil {
		t.Fatal(e)
	}
	active, e := s.ActivatePin(context.Background(), p, pin.PinID)
	if e != nil {
		t.Fatal(e)
	}
	retry, e := s.ActivatePin(context.Background(), p, pin.PinID)
	if e != nil || retry != active {
		t.Fatal("activation lost acknowledgment", e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '40 days' WHERE id=$1 OR id IN(SELECT resource_id FROM storage_snapshot_refs WHERE snapshot_id=$1)", snapshot.SnapshotID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	if _, e = s.GetSnapshot(context.Background(), p, snapshot.SnapshotID); e != nil {
		t.Fatal("release snapshot collected", e)
	}
	if e = s.ReleasePin(context.Background(), p, pin.PinID); e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	_, e = s.GetSnapshot(context.Background(), p, snapshot.SnapshotID)
	wantStatus(t, e, 409)
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	for _, ref := range snapshot.Resources {
		o, e := getObject(context.Background(), s.DB.Pool, ref.ResourceID)
		if e != nil || o.State != "deleted" {
			t.Fatal("child not collected after parent", e)
		}
	}
}
