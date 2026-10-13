package storage

import (
	"bufio"
	"context"
	"fmt"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type blockedReadStartupBlob struct {
	Blob
	started chan struct{}
}

func (b *blockedReadStartupBlob) Read(ctx context.Context, _ Physical) (io.ReadCloser, error) {
	close(b.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestIntegrationHTTPReadLeaseBoundsSlowS3Startup(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	resource := committed(t, s, p, scope, "startup content must not be returned")
	s.Config.ReadLease = time.Second

	blob := &blockedReadStartupBlob{Blob: s.Blob, started: make(chan struct{})}
	s.Blob = blob
	server := httptest.NewServer(s.Handler())
	defer server.Close()
	request, e := http.NewRequest(http.MethodGet, server.URL+"/internal/v1/resources/"+resource.ResourceID+"/content", nil)
	if e != nil {
		t.Fatal(e)
	}
	request.Header.Set("Authorization", "Bearer "+s.Config.Credentials[0].Token)
	type result struct {
		response *http.Response
		err      error
	}
	done := make(chan result, 1)
	go func() {
		response, err := http.DefaultClient.Do(request)
		done <- result{response, err}
	}()
	select {
	case <-blob.started:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP read did not reach the blocked S3 startup")
	}
	var got result
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("slow S3 startup remained blocked past the read lease")
	}
	if got.err != nil {
		t.Fatal("HTTP request failed before receiving the bounded startup error:", got.err)
	}
	defer got.response.Body.Close()
	if got.response.StatusCode != http.StatusInternalServerError {
		t.Fatalf("slow S3 startup returned status %d, want 500", got.response.StatusCode)
	}
	responseBody, e := io.ReadAll(got.response.Body)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(responseBody), "startup content must not be returned") {
		t.Fatal("HTTP returned object bytes after the read lease expired during S3 startup")
	}
	var leases int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_read_leases WHERE object_id=$1", resource.ResourceID).Scan(&leases); e != nil || leases != 0 {
		t.Fatal("timed-out S3 startup retained a read lease", leases, e)
	}
}

func TestIntegrationHTTPReadLeaseBoundsStalledConsumer(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	body := strings.Repeat("x", 16<<20)
	r := committed(t, s, p, scope, body)
	// Warm the real S3 GetObject path before shortening the lease. This test is
	// about a stalled HTTP consumer, not backend startup latency.
	warm, _, _, e := s.OpenRead(context.Background(), p, r.ResourceID, false)
	if e != nil {
		t.Fatal("warm S3 read failed:", e)
	}
	if _, e = io.Copy(io.Discard, warm); e != nil {
		_ = warm.Close()
		t.Fatal("drain warm S3 read:", e)
	}
	if e = warm.Close(); e != nil {
		t.Fatal("close warm S3 read:", e)
	}
	s.Config.ReadLease = 100 * time.Millisecond
	done := make(chan struct{})
	handler := s.Handler()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { defer close(done); handler.ServeHTTP(w, r) }))
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
	if e = conn.SetReadBuffer(1024); e != nil {
		t.Fatal(e)
	}
	_, e = fmt.Fprintf(conn, "GET /internal/v1/resources/%s/content HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nConnection: close\r\n\r\n", r.ResourceID, addr, s.Config.Credentials[0].Token)
	if e != nil {
		t.Fatal(e)
	}
	response, e := http.ReadResponse(bufio.NewReader(conn), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 || response.ContentLength != int64(len(body)) {
		t.Fatal("stream did not start successfully", response.Status)
	}
	first := make([]byte, 1)
	if _, e = io.ReadFull(response.Body, first); e != nil {
		t.Fatal("stream sent no initial content", e)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		conn.Close()
		t.Fatal("handler remained blocked writing after the read lease expired")
	}
	var leases int
	if e = s.DB.Pool.QueryRow(context.Background(), "SELECT count(*) FROM storage_read_leases WHERE object_id=$1", r.ResourceID).Scan(&leases); e != nil || leases != 0 {
		t.Fatal("stalled HTTP stream retained read lease", leases, e)
	}
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	bytes, _ := io.ReadAll(response.Body)
	if 1+len(bytes) >= len(body) {
		t.Fatal("stalled client received full object after lease deadline")
	}
}
