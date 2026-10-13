package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

type livenessBlob struct {
	Blob
	findErrors   map[string]bool
	findValues   map[string]Physical
	blockFind    bool
	deleteErrors map[string]bool
	allowDeletes map[string]bool
	denyUnlisted bool
	deleteCalls  []Physical
	pages        map[string]inventoryPage
	emptyPage    inventoryPage
	emptyCalls   int
	abortCalls   int
	started      chan struct{}
	startOnce    sync.Once
}

type inventoryPage struct {
	items                []Physical
	nextKey, nextVersion string
	err                  error
}

func (b *livenessBlob) Find(ctx context.Context, key string) (Physical, bool, error) {
	b.startOnce.Do(func() {
		if b.started != nil {
			close(b.started)
		}
	})
	if b.blockFind {
		<-ctx.Done()
		return Physical{}, false, ctx.Err()
	}
	if b.findErrors[key] {
		return Physical{}, false, errors.New("injected transient HEAD failure")
	}
	if p, ok := b.findValues[key]; ok {
		return p, true, nil
	}
	return Physical{}, false, nil
}

func (b *livenessBlob) Delete(ctx context.Context, p Physical) error {
	b.deleteCalls = append(b.deleteCalls, p)
	if b.deleteErrors[p.Key] {
		return errors.New("injected transient version delete failure")
	}
	if b.denyUnlisted && !b.allowDeletes[p.Key] {
		return errors.New("unexpected physical deletion blocked by test")
	}
	return nil
}

func (b *livenessBlob) Inventory(ctx context.Context, key, version string) ([]Physical, string, string, error) {
	page, ok := b.pages[key]
	if key == "" && !ok {
		if b.emptyCalls == 0 {
			b.emptyCalls++
			page, ok = b.emptyPage, true
		} else {
			return nil, "", "", nil
		}
	}
	if !ok {
		return nil, "", "", nil
	}
	return page.items, page.nextKey, page.nextVersion, page.err
}

func (b *livenessBlob) AbortOldMultipart(context.Context, map[string]bool, time.Time) error {
	b.abortCalls++
	return nil
}

func integrationPrincipal(s *Service) v.Principal {
	c := s.Config.Credentials[0]
	return v.Principal{Subject: c.Subject, Domain: c.Domain}
}

func seedDeletingObject(t *testing.T, s *Service, key, version string, created time.Time) string {
	t.Helper()
	oid := id()
	_, e := s.DB.Pool.Exec(context.Background(), "INSERT INTO storage_objects "+
		"(id,workspace,project,class,state,hash,size,physical_key,version,created_at) "+
		"VALUES($1,$2,$3,'resource','deleting',$4,1,$5,$6,$7)",
		oid, id(), id(), strings.Repeat("0", 64), key, version, created)
	if e != nil {
		t.Fatal(e)
	}
	return oid
}

func cleanupStorageFixture(t *testing.T, s *Service, objectIDs, keys []string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = s.DB.Pool.Exec(ctx, "DELETE FROM storage_uploads WHERE object_id=ANY($1)", objectIDs)
		_, _ = s.DB.Pool.Exec(ctx, "DELETE FROM storage_resources WHERE id=ANY($1)", objectIDs)
		_, _ = s.DB.Pool.Exec(ctx, "DELETE FROM storage_deletion_history WHERE id=ANY($1)", objectIDs)
		_, _ = s.DB.Pool.Exec(ctx, "DELETE FROM storage_objects WHERE id=ANY($1)", objectIDs)
		_, _ = s.DB.Pool.Exec(ctx, "DELETE FROM storage_physical_versions WHERE physical_key=ANY($1)", keys)
	})
}

func resetGCCursor(t *testing.T, s *Service) {
	t.Helper()
	var key, version string
	if e := s.DB.Pool.QueryRow(context.Background(), "SELECT physical_key,version FROM storage_gc_cursor WHERE id=1").Scan(&key, &version); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = s.DB.Pool.Exec(ctx, "UPDATE storage_gc_cursor SET physical_key=$1,version=$2 WHERE id=1", key, version)
	})
	if _, e := s.DB.Pool.Exec(context.Background(), "UPDATE storage_gc_cursor SET physical_key='',version='' WHERE id=1"); e != nil {
		t.Fatal(e)
	}
}

func TestIntegrationHTTPUploadBodyBoundedByWriterLease(t *testing.T) {
	s, _ := integration(t)
	p := integrationPrincipal(s)
	ctx := context.Background()
	body := "payload-long"
	s.Config.UploadLease = 10 * time.Second
	u, e := s.CreateUpload(ctx, p, v.CreateUploadRequest{
		Scope: v.Scope{WorkspaceID: id(), ProjectID: id()}, Class: v.ClassResource,
		SHA256: fingerprintBytes(body), SizeBytes: int64(len(body)), IdempotencyKey: id(),
	})
	if e != nil {
		t.Fatal(e)
	}
	cleanupStorageFixture(t, s, []string{u.UploadID}, nil)
	if _, e = s.DB.Pool.Exec(ctx, "UPDATE storage_uploads SET lease_until=clock_timestamp()+interval '700 milliseconds' WHERE id=$1", u.UploadID); e != nil {
		t.Fatal(e)
	}

	done := make(chan struct{})
	handler := s.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	addr := strings.TrimPrefix(server.URL, "http://")
	remote, e := net.ResolveTCPAddr("tcp", addr)
	if e != nil {
		t.Fatal(e)
	}
	conn, e := net.DialTCP("tcp", nil, remote)
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, e = fmt.Fprintf(conn, "PUT /internal/v1/uploads/%s/content HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nX-Writer-Token: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
		u.UploadID, addr, s.Config.Credentials[0].Token, u.WriterToken, len(body))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = conn.Write([]byte(body[:3])); e != nil {
		t.Fatal(e)
	}
	response, e := http.ReadResponse(bufio.NewReader(conn), nil)
	if e != nil {
		t.Fatalf("valid authorized partial upload did not stop at its writer lease: %v", e)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized {
		t.Fatal("request was rejected before exercising the valid upload lease")
	}
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("expired partial upload returned %s, want 409", response.Status)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler remained blocked after writer lease expiry")
	}
	var state string
	if e = s.DB.Pool.QueryRow(ctx, "SELECT state FROM storage_uploads WHERE id=$1", u.UploadID).Scan(&state); e != nil || state != "created" {
		t.Fatalf("interrupted body advanced upload state to %q: %v", state, e)
	}
	var scratch, versions int
	if e = s.DB.Pool.QueryRow(ctx, "SELECT count(*) FROM storage_scratch WHERE upload_id=$1", u.UploadID).Scan(&scratch); e != nil || scratch != 0 {
		t.Fatalf("interrupted body retained scratch rows: %d %v", scratch, e)
	}
	if e = s.DB.Pool.QueryRow(ctx, "SELECT count(*) FROM storage_physical_versions WHERE object_id=$1", u.UploadID).Scan(&versions); e != nil || versions != 0 {
		t.Fatalf("interrupted body wrote a physical version receipt: %d %v", versions, e)
	}
}

func TestIntegrationHTTPUploadFailsClosedWithoutReadDeadline(t *testing.T) {
	s, _ := integration(t)
	p := integrationPrincipal(s)
	body := "content"
	u, e := s.CreateUpload(context.Background(), p, v.CreateUploadRequest{
		Scope: v.Scope{WorkspaceID: id(), ProjectID: id()}, Class: v.ClassResource,
		SHA256: fingerprintBytes(body), SizeBytes: int64(len(body)), IdempotencyKey: id(),
	})
	if e != nil {
		t.Fatal(e)
	}
	cleanupStorageFixture(t, s, []string{u.UploadID}, nil)
	request := httptest.NewRequest("PUT", "/internal/v1/uploads/"+u.UploadID+"/content", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+s.Config.Credentials[0].Token)
	request.Header.Set("X-Writer-Token", u.WriterToken)
	response := httptest.NewRecorder()
	s.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsupported read deadline was not rejected: %d %s", response.Code, response.Body.String())
	}
	var state string
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT state FROM storage_uploads WHERE id=$1", u.UploadID).Scan(&state); e != nil || state != "created" {
		t.Fatalf("fail-closed upload changed state to %q: %v", state, e)
	}
	var scratch int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_scratch WHERE upload_id=$1", u.UploadID).Scan(&scratch); e != nil || scratch != 0 {
		t.Fatalf("unsupported deadline created scratch: %d %v", scratch, e)
	}
}

type putResult struct {
	upload v.Upload
	err    error
}

type gatedUploadBody struct {
	source       *strings.Reader
	started      chan struct{}
	continueRead chan struct{}
	once         sync.Once
}

func (r *gatedUploadBody) Read(p []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
		<-r.continueRead
	})
	return r.source.Read(p)
}

type putCountingBlob struct {
	Blob
	putCalls int
}

func (b *putCountingBlob) Put(ctx context.Context, key string, f *os.File, hash string, size int64) (Physical, error) {
	b.putCalls++
	return b.Blob.Put(ctx, key, f, hash, size)
}

func TestIntegrationExpiredRecoveryGCRejectsPUTBeforeReadingBody(t *testing.T) {
	s, p := integration(t)
	body := "must remain unread"
	s.Config.UploadLease = time.Hour
	u, e := s.CreateUpload(context.Background(), p, v.CreateUploadRequest{
		Scope: v.Scope{WorkspaceID: id(), ProjectID: id()}, Class: v.ClassRecovery,
		SHA256: fingerprintBytes(body), SizeBytes: int64(len(body)), IdempotencyKey: id(),
	})
	if e != nil {
		t.Fatal(e)
	}
	cleanupStorageFixture(t, s, []string{u.UploadID}, nil)
	if _, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_objects SET expires_at=clock_timestamp()-interval '1 second' WHERE id=$1", u.UploadID); e != nil {
		t.Fatal(e)
	}
	if e = s.GC(context.Background()); e != nil {
		t.Fatal("real GC did not process expired recovery upload:", e)
	}
	var objectState, uploadState string
	if e = s.DB.Pool.QueryRow(context.Background(), `SELECT o.state,u.state FROM storage_objects o JOIN storage_uploads u ON u.object_id=o.id WHERE o.id=$1`, u.UploadID).Scan(&objectState, &uploadState); e != nil || objectState != "deleted" || uploadState != "deleted" {
		t.Fatalf("GC did not fence expired recovery upload: object=%q upload=%q err=%v", objectState, uploadState, e)
	}
	blob := &putCountingBlob{Blob: s.Blob}
	s.Blob = blob
	reader := &gatedUploadBody{source: strings.NewReader(body), started: make(chan struct{}), continueRead: make(chan struct{})}
	done := make(chan putResult, 1)
	go func() {
		got, putErr := s.Put(context.Background(), p, u.UploadID, u.WriterToken, reader)
		done <- putResult{got, putErr}
	}()

	select {
	case <-reader.started:
		var scratch int
		if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_scratch WHERE upload_id=$1", u.UploadID).Scan(&scratch); e != nil {
			close(reader.continueRead)
			<-done
			t.Fatal(e)
		}
		close(reader.continueRead)
		result := <-done
		if result.err == nil {
			t.Fatal("GC-fenced recovery upload unexpectedly succeeded")
		}
		t.Fatalf("GC-fenced PUT read body and created %d scratch row(s) before rejecting", scratch)
	case result := <-done:
		wantStatus(t, result.err, 409)
		var uploadErr *fault
		if !errors.As(result.err, &uploadErr) || uploadErr.Code != "writer_fenced" {
			t.Fatalf("GC-fenced PUT returned %v, want writer_fenced", result.err)
		}
	case <-time.After(2 * time.Second):
		close(reader.continueRead)
		<-done
		t.Fatal("GC-fenced PUT did not return promptly")
	}
	if blob.putCalls != 0 {
		t.Fatalf("GC-fenced PUT wrote %d S3 object(s)", blob.putCalls)
	}
	var scratch int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_scratch WHERE upload_id=$1", u.UploadID).Scan(&scratch); e != nil || scratch != 0 {
		t.Fatalf("GC-fenced PUT retained scratch row(s): %d %v", scratch, e)
	}
	var versions int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_physical_versions WHERE object_id=$1", u.UploadID).Scan(&versions); e != nil || versions != 0 {
		t.Fatalf("GC-fenced PUT recorded %d physical version(s): %v", versions, e)
	}
}

func TestIntegrationUploadS3FindBoundedByWriterLease(t *testing.T) {
	s, p := integration(t)
	s.Config.UploadLease = 10 * time.Second
	body := "complete-body"
	u, e := s.CreateUpload(context.Background(), p, v.CreateUploadRequest{
		Scope: v.Scope{WorkspaceID: id(), ProjectID: id()}, Class: v.ClassResource,
		SHA256: fingerprintBytes(body), SizeBytes: int64(len(body)), IdempotencyKey: id(),
	})
	if e != nil {
		t.Fatal(e)
	}
	cleanupStorageFixture(t, s, []string{u.UploadID}, nil)
	if _, e = s.DB.Pool.Exec(context.Background(), "UPDATE storage_uploads SET lease_until=clock_timestamp()+interval '500 milliseconds' WHERE id=$1", u.UploadID); e != nil {
		t.Fatal(e)
	}

	blob := &livenessBlob{Blob: s.Blob, blockFind: true}
	blob.started = make(chan struct{})
	s.Blob = blob
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan putResult, 1)
	started := time.Now()
	go func() {
		got, putErr := s.Put(ctx, p, u.UploadID, u.WriterToken, strings.NewReader(body))
		result <- putResult{got, putErr}
	}()
	select {
	case <-time.After(time.Second):
		cancel()
		<-result
		t.Fatal("S3 HEAD did not start")
	case <-blob.started:
	}
	select {
	case got := <-result:
		if got.err == nil {
			t.Fatal("upload succeeded after its lease expired during S3 HEAD")
		}
		if time.Since(started) > 900*time.Millisecond {
			t.Fatalf("S3 HEAD was not bounded near the 500ms writer lease: %s", time.Since(started))
		}
	case <-time.After(1200 * time.Millisecond):
		cancel()
		<-result
		t.Fatal("upload remained blocked in S3 HEAD after writer lease expiry")
	}
	var scratch int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_scratch WHERE upload_id=$1", u.UploadID).Scan(&scratch); e != nil || scratch != 0 {
		t.Fatalf("expired S3 operation retained scratch rows: %d %v", scratch, e)
	}
}

func TestIntegrationCatalogGCFairRetryAndTransientFind(t *testing.T) {
	s, _ := integration(t)
	ctx := context.Background()
	resetGCCursor(t, s)
	s.Config.OrphanGrace = 100 * 365 * 24 * time.Hour
	failKeys := map[string]bool{}
	objectIDs, keys := []string{}, []string{}
	ancient := time.Now().Add(-200 * 365 * 24 * time.Hour)
	for i := 0; i < 100; i++ {
		key := "storage/resource/gc-fair/" + id()
		failKeys[key] = true
		objectIDs = append(objectIDs, seedDeletingObject(t, s, key, "version", ancient.Add(time.Duration(i)*time.Second)))
		keys = append(keys, key)
		if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,created_at) VALUES($1,'version',$2)", key, ancient); e != nil {
			t.Fatal(e)
		}
	}
	goodKey := "storage/resource/gc-fair/" + id()
	goodID := seedDeletingObject(t, s, goodKey, "version", ancient.Add(24*time.Hour))
	objectIDs = append(objectIDs, goodID)
	keys = append(keys, goodKey)
	if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,created_at) VALUES($1,'version',$2)", goodKey, ancient); e != nil {
		t.Fatal(e)
	}

	liveKey := "storage/resource/gc-live/" + id()
	liveID := id()
	_, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_objects "+
		"(id,workspace,project,class,state,hash,size,physical_key,version,created_at) "+
		"VALUES($1,$2,$3,'resource','ready',$4,1,$5,'live-version',clock_timestamp())",
		liveID, id(), id(), strings.Repeat("1", 64), liveKey)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB.Pool.Exec(ctx, "INSERT INTO storage_resources(id) VALUES($1)", liveID); e != nil {
		t.Fatal(e)
	}
	objectIDs = append(objectIDs, liveID)
	keys = append(keys, liveKey)
	cleanupStorageFixture(t, s, objectIDs, keys)

	blob := &livenessBlob{Blob: s.Blob, deleteErrors: failKeys, allowDeletes: map[string]bool{goodKey: true}, denyUnlisted: true}
	s.Blob = blob
	for range 2 {
		if e = s.GC(ctx); e != nil {
			t.Fatal("transient object failure blocked GC batch:", e)
		}
	}
	var state string
	if e = s.DB.Pool.QueryRow(ctx, "SELECT state FROM storage_objects WHERE id=$1", goodID).Scan(&state); e != nil || state != "deleted" {
		t.Fatalf("later eligible object starved behind 100 persistent failures: state=%q err=%v", state, e)
	}
	for _, p := range blob.deleteCalls {
		if p.Key == liveKey {
			t.Fatal("GC deleted a live referenced physical version")
		}
	}
	if e = s.DB.Pool.QueryRow(ctx, "SELECT state FROM storage_objects WHERE id=$1", objectIDs[0]).Scan(&state); e != nil || state != "deleting" {
		t.Fatalf("failed object was not left retryable: state=%q err=%v", state, e)
	}
}

func TestIntegrationCatalogGCTransientFindDoesNotBlockBatch(t *testing.T) {
	s, _ := integration(t)
	ctx := context.Background()
	resetGCCursor(t, s)
	s.Config.OrphanGrace = 100 * 365 * 24 * time.Hour
	ancient := time.Now().Add(-200 * 365 * 24 * time.Hour)
	missingKey := "storage/resource/gc-find/" + id()
	laterKey := "storage/resource/gc-find/" + id()
	missingID := seedDeletingObject(t, s, missingKey, "", ancient.Add(-time.Hour))
	laterID := seedDeletingObject(t, s, laterKey, "later-version", ancient)
	if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,created_at) VALUES($1,'found-version',$2)", missingKey, ancient); e != nil {
		t.Fatal(e)
	}
	if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,created_at) VALUES($1,'later-version',$2)", laterKey, ancient); e != nil {
		t.Fatal(e)
	}
	cleanupStorageFixture(t, s, []string{missingID, laterID}, []string{missingKey, laterKey})
	blob := &livenessBlob{
		Blob: s.Blob, findErrors: map[string]bool{missingKey: true},
		findValues:   map[string]Physical{missingKey: {Key: missingKey, Version: "found-version"}},
		allowDeletes: map[string]bool{missingKey: true, laterKey: true}, denyUnlisted: true,
	}
	s.Blob = blob
	if e := s.GC(ctx); e != nil {
		t.Fatal("transient HEAD failure blocked unrelated catalog candidates:", e)
	}
	var state string
	if e := s.DB.Pool.QueryRow(ctx, "SELECT state FROM storage_objects WHERE id=$1", laterID).Scan(&state); e != nil || state != "deleted" {
		t.Fatalf("later catalog candidate was skipped after HEAD failure: state=%q err=%v", state, e)
	}
	blob.findErrors[missingKey] = false
	if e := s.GC(ctx); e != nil {
		t.Fatal(e)
	}
	if e := s.DB.Pool.QueryRow(ctx, "SELECT state FROM storage_objects WHERE id=$1", missingID).Scan(&state); e != nil || state != "deleted" {
		t.Fatalf("failed missing-version object did not recover on a later attempt: state=%q err=%v", state, e)
	}
}

func TestIntegrationInventoryFailureAdvancesAndRetriesAfterRestart(t *testing.T) {
	s, _ := integration(t)
	resetGCCursor(t, s)
	s.Config.OrphanGrace = time.Millisecond
	ctx := context.Background()
	keyA := "storage/resource/inventory-retry/" + id()
	keyB := "storage/resource/inventory-retry/" + id()
	blob := &livenessBlob{
		Blob:         s.Blob,
		deleteErrors: map[string]bool{keyA: true},
		allowDeletes: map[string]bool{keyA: true, keyB: true},
		denyUnlisted: true,
		emptyPage: inventoryPage{
			items:   []Physical{{Key: keyA, Version: "version-a"}},
			nextKey: "page-2", nextVersion: "marker-2",
		},
		pages: map[string]inventoryPage{
			"page-2": {items: []Physical{{Key: keyB, Version: "version-b"}}},
		},
	}
	s.Blob = blob
	for _, item := range []Physical{{Key: keyA, Version: "version-a"}, {Key: keyB, Version: "version-b"}} {
		if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_physical_versions(physical_key,version,created_at) VALUES($1,$2,clock_timestamp()-interval '1 hour') ON CONFLICT DO NOTHING", item.Key, item.Version); e != nil {
			t.Fatal(e)
		}
	}

	if e := s.inventory(ctx); e != nil {
		t.Fatal("one physical-version failure stopped the scan:", e)
	}
	var cursorKey, cursorVersion string
	if e := s.DB.Pool.QueryRow(ctx, "SELECT physical_key,version FROM storage_gc_cursor WHERE id=1").Scan(&cursorKey, &cursorVersion); e != nil {
		t.Fatal(e)
	}
	if cursorKey != "page-2" || cursorVersion != "marker-2" {
		t.Fatalf("failed version prevented durable cursor progress: got %q/%q", cursorKey, cursorVersion)
	}
	var retryPending bool
	if e := s.DB.Pool.QueryRow(ctx, "SELECT state='ready' AND inventory_retry_at IS NOT NULL FROM storage_physical_versions WHERE physical_key=$1 AND version='version-a'", keyA).Scan(&retryPending); e != nil || !retryPending {
		t.Fatalf("failed exact version was not durably queued: pending=%v err=%v", retryPending, e)
	}

	restarted := &Service{DB: s.DB, Blob: blob, Owner: s.Owner, Config: s.Config}
	if e := restarted.inventory(ctx); e != nil {
		t.Fatal("persistent version failure stopped the next scan:", e)
	}
	if countKey(blob.deleteCalls, keyA) != 2 || countKey(blob.deleteCalls, keyB) != 1 {
		t.Fatalf("persistent A failure prevented later B progress: A=%d B=%d", countKey(blob.deleteCalls, keyA), countKey(blob.deleteCalls, keyB))
	}
	if blob.abortCalls < 2 {
		t.Fatalf("multipart cleanup did not run after repeated version failure: %d calls", blob.abortCalls)
	}

	liveID := id()
	if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_objects "+
		"(id,workspace,project,class,state,hash,size,physical_key,version,created_at) "+
		"VALUES($1,$2,$3,'resource','staging',$4,1,$5,'',clock_timestamp())",
		liveID, id(), id(), strings.Repeat("2", 64), keyA); e != nil {
		t.Fatal(e)
	}
	if _, e := s.DB.Pool.Exec(ctx, "INSERT INTO storage_uploads(id,object_id,writer,lease_until,state,creator) "+
		"VALUES($1,$1,$2,clock_timestamp()+interval '1 hour','writing','test')", liveID, id()); e != nil {
		t.Fatal(e)
	}
	cleanupStorageFixture(t, s, []string{liveID}, []string{keyA, keyB})

	restarted = &Service{DB: s.DB, Blob: blob, Owner: s.Owner, Config: s.Config}
	if e := restarted.inventory(ctx); e != nil {
		t.Fatal("restarted inventory worker failed:", e)
	}
	if countKey(blob.deleteCalls, keyA) != 2 {
		t.Fatalf("retry deleted a version protected by a live writer: %d calls", countKey(blob.deleteCalls, keyA))
	}
	if countKey(blob.deleteCalls, keyB) != 1 {
		t.Fatalf("later inventory page was not processed: %d delete calls", countKey(blob.deleteCalls, keyB))
	}
	if blob.abortCalls < 3 {
		t.Fatalf("multipart cleanup did not run after the failed version: %d calls", blob.abortCalls)
	}

	if _, e := s.DB.Pool.Exec(ctx, "UPDATE storage_uploads SET lease_until=clock_timestamp()-interval '1 second' WHERE object_id=$1", liveID); e != nil {
		t.Fatal(e)
	}
	blob.deleteErrors[keyA] = false
	restarted = &Service{DB: s.DB, Blob: blob, Owner: s.Owner, Config: s.Config}
	if e := restarted.inventory(ctx); e != nil {
		t.Fatal("durable physical-version retry failed after writer expiry:", e)
	}
	if countKey(blob.deleteCalls, keyA) != 3 {
		t.Fatalf("failed version was not durably retried after restart: %d calls", countKey(blob.deleteCalls, keyA))
	}
	var state string
	if e := s.DB.Pool.QueryRow(ctx, "SELECT state FROM storage_physical_versions WHERE physical_key=$1 AND version='version-a'", keyA).Scan(&state); e != nil || state != "deleted" {
		t.Fatalf("successful exact-version retry was not recorded: state=%q err=%v", state, e)
	}
}

func countKey(items []Physical, key string) int {
	n := 0
	for _, p := range items {
		if p.Key == key {
			n++
		}
	}
	return n
}
