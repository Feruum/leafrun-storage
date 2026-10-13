package storage

import (
	"context"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"testing"
	"time"
)

var testTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const hour = time.Hour

func TestArtifactPublicationFailsClosed(t *testing.T) {
	s := &Service{}
	if e := s.publication(context.Background(), v.Principal{}, v.Artifact{}); e == nil {
		t.Fatal("unconfigured owner should reject public artifact")
	}
}
func TestResultDeadlineDominatesPin(t *testing.T) {
	if retentionDeadline(v.ClassResult, "ordinary", testTime, testTime.Add(24*hour)).After(testTime.Add(24 * hour)) {
		t.Fatal("pin extends artifact deadline")
	}
}
