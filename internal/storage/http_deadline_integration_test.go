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

func TestIntegrationHTTPReadLeaseBoundsStalledConsumer(t *testing.T) {
	s, p := integration(t)
	scope := v.Scope{WorkspaceID: id(), ProjectID: id()}
	body := strings.Repeat("x", 16<<20)
	r := committed(t, s, p, scope, body)
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
