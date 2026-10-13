package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"os"
	"time"
)

type Credential struct {
	Token   string `json:"token"`
	Subject string `json:"subject"`
	Domain  string `json:"domain"`
}
type Config struct {
	Listen, DatabaseURL, Bucket, Endpoint, OwnerURL, OwnerToken, TemporalAddress, TemporalNamespace, TaskQueue, Scratch string
	Credentials                                                                                                         []Credential
	UploadLease, ReadLease, PendingLease, OrphanGrace, SweepInterval                                                    time.Duration
	MaxUpload                                                                                                           int64
}

func env(k, d string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return d
}
func LoadConfig() (Config, error) {
	c := Config{Listen: env("LEAFRUN_STORAGE_LISTEN", ":8081"), DatabaseURL: os.Getenv("LEAFRUN_STORAGE_DATABASE_URL"), Bucket: os.Getenv("LEAFRUN_STORAGE_S3_BUCKET"), Endpoint: os.Getenv("LEAFRUN_STORAGE_S3_ENDPOINT"), OwnerURL: os.Getenv("LEAFRUN_STORAGE_OWNER_URL"), OwnerToken: os.Getenv("LEAFRUN_STORAGE_OWNER_TOKEN"), TemporalAddress: env("LEAFRUN_STORAGE_TEMPORAL_ADDRESS", "localhost:7233"), TemporalNamespace: env("LEAFRUN_STORAGE_TEMPORAL_NAMESPACE", "default"), TaskQueue: env("LEAFRUN_STORAGE_TEMPORAL_TASK_QUEUE", "leafrun-storage-gc"), Scratch: env("LEAFRUN_STORAGE_SCRATCH_DIR", os.TempDir()), UploadLease: 5 * time.Minute, ReadLease: 5 * time.Minute, PendingLease: 10 * time.Minute, OrphanGrace: 24 * time.Hour, MaxUpload: 1 << 30}
	if err := json.Unmarshal([]byte(os.Getenv("LEAFRUN_STORAGE_SERVICE_CREDENTIALS")), &c.Credentials); err != nil {
		return c, fmt.Errorf("service credentials: %w", err)
	}
	if len(c.Credentials) == 0 || c.DatabaseURL == "" || c.Bucket == "" || c.OwnerURL == "" || c.OwnerToken == "" {
		return c, fmt.Errorf("database, private S3 bucket, owner adapter and service credentials required")
	}
	for _, a := range c.Credentials {
		if len(a.Token) < 16 || a.Subject == "" || a.Domain == "" {
			return c, fmt.Errorf("invalid credential")
		}
	}
	var err error
	c.SweepInterval, err = time.ParseDuration(env("LEAFRUN_STORAGE_SWEEP_INTERVAL", "5m"))
	if err != nil || c.SweepInterval < time.Second || c.SweepInterval > 24*time.Hour || c.SweepInterval%time.Second != 0 {
		return c, fmt.Errorf("sweep interval must be whole seconds from 1s to 24h")
	}
	return c, nil
}
func id() string {
	var b [24]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b[:])
}
func fingerprint(x any) string {
	b, _ := json.Marshal(x)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func validScope(s v.Scope) bool { return s.WorkspaceID != "" && s.ProjectID != "" }
func validHash(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}

type fault struct {
	Status        int
	Code, Message string
}

func (e *fault) Error() string           { return e.Message }
func fail(status int, code string) error { return &fault{status, code, code} }
func bindings(m v.SnapshotManifest, refs []v.ResourceRef) (map[string]v.SnapshotFile, error) {
	if e := v.ValidateManifest(m); e != nil {
		return nil, fail(400, "invalid_manifest")
	}
	if refs == nil {
		return nil, fail(400, "null_bindings")
	}
	want := map[string]v.SnapshotFile{}
	for _, f := range m.Files {
		want["project:0:"+f.Path] = f
	}
	for i, font := range m.Fonts {
		if font.InProject {
			continue
		}
		for _, f := range font.Files {
			want[fmt.Sprintf("font:%d:%s", i, f.Path)] = f
		}
	}
	seen := map[string]bool{}
	for _, r := range refs {
		k := fmt.Sprintf("%s:%d:%s", r.Tree, r.FontIndex, r.Path)
		if _, ok := want[k]; !ok || seen[k] || r.ResourceID == "" {
			return nil, fail(400, "invalid_bindings")
		}
		seen[k] = true
	}
	if len(seen) != len(want) {
		return nil, fail(400, "incomplete_bindings")
	}
	return want, nil
}
