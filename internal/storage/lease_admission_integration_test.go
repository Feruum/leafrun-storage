package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIntegrationReadLeaseDoesNotSlideDuringDatabaseWait(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	r := committed(t, s, p, scope, "fixed deadline")
	s.Config.ReadLease = 100 * time.Millisecond
	lock, e := s.DB.Pool.Begin(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Rollback(context.Background())
	if _, e = lock.Exec(context.Background(), "SELECT pg_advisory_xact_lock(710032)"); e != nil {
		t.Fatal(e)
	}
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		body, _, _, e := s.OpenRead(context.Background(), p, r.ResourceID, false)
		if body != nil {
			body.Close()
		}
		done <- e
	}()
	<-started
	time.Sleep(300 * time.Millisecond)
	if e = lock.Commit(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e == nil {
		t.Fatal("database wait restarted the read lifetime")
	}
	var leases int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_read_leases WHERE object_id=$1", r.ResourceID).Scan(&leases); e != nil || leases != 0 {
		t.Fatal("already-expired admission retained a lease", leases, e)
	}
}
func TestIntegrationHTTPContentRequiresWriteDeadlineSupport(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	r := committed(t, s, p, scope, "private")
	request := httptest.NewRequest("GET", "/internal/v1/resources/"+r.ResourceID+"/content", nil)
	request.Header.Set("Authorization", "Bearer "+s.Config.Credentials[0].Token)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, request)
	if w.Code != 503 {
		t.Fatal("unsupported response writer exposed unbounded content", w.Code)
	}
	var count int
	if e := s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_read_leases WHERE object_id=$1", r.ResourceID).Scan(&count); e != nil || count != 0 {
		t.Fatal("unsupported writer retained lease", count, e)
	}
}
