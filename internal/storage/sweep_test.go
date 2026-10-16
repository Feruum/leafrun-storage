package storage

import (
	"testing"
	"time"
)

func configFixture(t *testing.T) {
	t.Helper()
	t.Setenv("LEAFRUN_STORAGE_AUTH_MODE", "dev")
	t.Setenv("LEAFRUN_STORAGE_DEV_FIXTURES", "true")
	t.Setenv("LEAFRUN_STORAGE_DATABASE_URL", "postgres://local/storage")
	t.Setenv("LEAFRUN_STORAGE_S3_BUCKET", "private")
	t.Setenv("LEAFRUN_STORAGE_OWNER_URL", "http://owner")
	t.Setenv("LEAFRUN_STORAGE_OWNER_TOKEN", "owner-secret")
	t.Setenv("LEAFRUN_STORAGE_SERVICE_CREDENTIALS", `[{"token":"service-token-long","subject":"core","domain":"core"}]`)
}
func TestSweepIntervalConfiguration(t *testing.T) {
	configFixture(t)
	for _, tc := range []struct {
		input string
		want  time.Duration
	}{{"", 5 * time.Minute}, {"7m", 7 * time.Minute}, {"61s", 61 * time.Second}, {"24h", 24 * time.Hour}} {
		t.Setenv("LEAFRUN_STORAGE_SWEEP_INTERVAL", tc.input)
		c, e := LoadConfig()
		if e != nil || c.SweepInterval != tc.want {
			t.Fatalf("interval %q: got %v, %v", tc.input, c.SweepInterval, e)
		}
	}
	for _, input := range []string{"0", "0s", "-1s", "100ms", "24h1s", "invalid"} {
		t.Setenv("LEAFRUN_STORAGE_SWEEP_INTERVAL", input)
		if _, e := LoadConfig(); e == nil {
			t.Fatalf("invalid interval %q accepted", input)
		}
	}
}
