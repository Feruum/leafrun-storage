package storage

import (
	"context"
	"testing"
)

func TestS3RejectsMutableLatestReceipt(t *testing.T) {
	s := &S3{}
	for _, p := range []Physical{{Key: "k"}, {Version: "v"}, {}} {
		if p.exact() {
			t.Fatal("incomplete physical receipt accepted")
		}
		if _, e := s.Read(context.Background(), p); e == nil {
			t.Fatal("mutable latest read allowed")
		}
		if e := s.Delete(context.Background(), p); e == nil {
			t.Fatal("mutable latest deletion allowed")
		}
	}
}
