package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"strings"
	"testing"
	"time"
)

func TestIntegrationReadDeadlineAndCancellation(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	r := committed(t, s, p, scope, "bounded read")
	s.Config.ReadLease = 25 * time.Millisecond
	body, _, _, e := s.OpenRead(context.Background(), p, r.ResourceID, false)
	if e != nil {
		t.Fatal(e)
	}
	time.Sleep(40 * time.Millisecond)
	_, e = io.ReadAll(body)
	if e == nil {
		t.Fatal("read continued after lease deadline")
	}
	ctx, cancel := context.WithCancel(context.Background())
	body, _, _, e = s.OpenRead(ctx, p, r.ResourceID, false)
	if e != nil {
		t.Fatal(e)
	}
	cancel()
	time.Sleep(20 * time.Millisecond)
	_, e = io.ReadAll(body)
	if e == nil {
		t.Fatal("cancel ignored")
	}
	var count int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_read_leases WHERE object_id=$1", r.ResourceID).Scan(&count); e != nil || count != 0 {
		t.Fatal("timed/canceled leases not released", count, e)
	}
}
func TestIntegrationLiveWriterAndOrphanGrace(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	u, e := s.CreateUpload(context.Background(), p, v.CreateUploadRequest{Scope: scope, Class: v.ClassResource, SHA256: fingerprintBytes("live"), SizeBytes: 4, IdempotencyKey: id()})
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET created_at=clock_timestamp()-interval '2 days' WHERE id=$1", u.UploadID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	current, e := s.GetUpload(context.Background(), p, u.UploadID)
	if e != nil || current.State != "created" {
		t.Fatal("live writer collected", e)
	}
	_, e = s.Put(context.Background(), p, u.UploadID, u.WriterToken, strings.NewReader("live"))
	if e != nil {
		t.Fatal(e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1", u.UploadID)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	o, e := getObject(context.Background(), s.DB.Pool, u.UploadID)
	if e != nil || o.State != "deleting" {
		t.Fatal("expired writer was not fenced", o.State, e)
	}
	_, ok, e := s.Blob.Find(context.Background(), o.Key)
	if e != nil || !ok {
		t.Fatal("new orphan deleted before grace", e)
	}
	_, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_physical_versions SET created_at=clock_timestamp()-interval '2 days' WHERE physical_key=$1", o.Key)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal(e)
	}
	o, e = getObject(context.Background(), s.DB.Pool, u.UploadID)
	if e != nil || o.State != "deleted" {
		t.Fatal("eligible orphan not collected", e)
	}
}
