package storage

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestReadLeaseClosesOnDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	released := false
	r := &leasedReader{body: io.NopCloser(strings.NewReader("bytes")), ctx: ctx, cancel: cancel, release: func() { released = true }}
	time.Sleep(3 * time.Millisecond)
	if _, e := r.Read(make([]byte, 1)); e == nil {
		t.Fatal("expired read continued")
	}
	if !released {
		t.Fatal("lease not released")
	}
}
