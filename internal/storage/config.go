package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"os"
	"time"
)

type Credential struct {
	Token   string   `json:"token"`
	Subject string   `json:"subject"`
	Domain  string   `json:"domain"`
	Actions []string `json:"actions,omitempty"`
}
type Config struct {
	ImportVerifierToken                                                                                                 string
	AuthMode                                                                                                            string
	DevFixtures                                                                                                         bool
	GuestCookieName                                                                                                     string
	CookieName                                                                                                          string
	Authenticator                                                                                                       authn.Authenticator
	Access                                                                                                              authn.Checker
	BrowserGuard                                                                                                        *authn.BrowserGuard
	AccessURL, AccessToken                                                                                              string
	Listen, DatabaseURL, Bucket, Endpoint, OwnerURL, OwnerToken, TemporalAddress, TemporalNamespace, TaskQueue, Scratch string
	Credentials                                                                                                         []Credential
	UploadLease, ReadLease, PendingLease, OrphanGrace, SweepInterval                                                    time.Duration
	MaxUpload                                                                                                           int64
	AuditRetention                                                                                                      time.Duration
}

func env(k, d string) string {
	if s := os.Getenv(k); s != "" {
		return s
	}
	return d
}
func LoadConfig() (Config, error) {
	c := Config{Listen: env("LEAFRUN_STORAGE_LISTEN", ":8081"), DatabaseURL: os.Getenv("LEAFRUN_STORAGE_DATABASE_URL"), Bucket: os.Getenv("LEAFRUN_STORAGE_S3_BUCKET"), Endpoint: os.Getenv("LEAFRUN_STORAGE_S3_ENDPOINT"), OwnerURL: os.Getenv("LEAFRUN_STORAGE_OWNER_URL"), OwnerToken: os.Getenv("LEAFRUN_STORAGE_OWNER_TOKEN"), TemporalAddress: env("LEAFRUN_STORAGE_TEMPORAL_ADDRESS", "localhost:7233"), TemporalNamespace: env("LEAFRUN_STORAGE_TEMPORAL_NAMESPACE", "default"), TaskQueue: env("LEAFRUN_STORAGE_TEMPORAL_TASK_QUEUE", "leafrun-storage-gc"), Scratch: env("LEAFRUN_STORAGE_SCRATCH_DIR", os.TempDir()), UploadLease: 5 * time.Minute, ReadLease: 5 * time.Minute, PendingLease: 10 * time.Minute, OrphanGrace: 24 * time.Hour, MaxUpload: 1 << 30}
	if err := jsonvalue.Unmarshal([]byte(os.Getenv("LEAFRUN_STORAGE_SERVICE_CREDENTIALS")), &c.Credentials, 64<<10); err != nil {
		return c, fmt.Errorf("invalid Storage service credentials")
	}
	c.AuthMode = os.Getenv("LEAFRUN_STORAGE_AUTH_MODE")
	c.ImportVerifierToken = os.Getenv("LEAFRUN_STORAGE_IMPORT_VERIFIER_TOKEN")
	if c.ImportVerifierToken != "" && (!accessv1.ValidCredential(c.ImportVerifierToken) || len(c.ImportVerifierToken) < 32 || c.ImportVerifierToken == c.OwnerToken) {
		return c, fmt.Errorf("invalid dedicated Storage evidence credential")
	}
	c.DevFixtures = os.Getenv("LEAFRUN_STORAGE_DEV_FIXTURES") == "true"
	c.CookieName = env("LEAFRUN_STORAGE_SESSION_COOKIE", authn.DefaultSessionCookieName)
	c.GuestCookieName = env("LEAFRUN_GUEST_COOKIE_NAME", authn.DefaultGuestCookieName)
	if err := authn.ConfiguredCookieNames(c.CookieName, c.GuestCookieName).Validate(); err != nil {
		return c, err
	}
	if c.AuthMode != "access" && c.AuthMode != "dev" {
		return c, fmt.Errorf("explicit Storage authentication mode required")
	}
	if c.AuthMode == "dev" && !c.DevFixtures {
		return c, fmt.Errorf("explicit Storage dev fixture gate required")
	}
	if c.AuthMode == "access" {
		if c.DevFixtures {
			return c, fmt.Errorf("Storage fixture fallback forbidden in Access mode")
		}
		c.AccessURL = os.Getenv("LEAFRUN_STORAGE_ACCESS_URL")
		c.AccessToken = os.Getenv("LEAFRUN_STORAGE_ACCESS_TOKEN")
		var origins []string
		if err := jsonvalue.Unmarshal([]byte(os.Getenv("LEAFRUN_STORAGE_BROWSER_ORIGINS_JSON")), &origins, 32<<10); err != nil {
			return c, fmt.Errorf("explicit Storage browser origins required")
		}
		var err error
		c.BrowserGuard, err = authn.NewBrowserGuard(origins, os.Getenv("LEAFRUN_STORAGE_LOOPBACK_HTTP") == "true")
		if err != nil {
			return c, err
		}
		client, err := authn.NewClientWithCookieNames(c.AccessURL, c.AccessToken, authn.ConfiguredCookieNames(c.CookieName, c.GuestCookieName), nil)
		if err != nil {
			return c, err
		}
		c.Authenticator = client
		c.Access = client
		if c.OwnerToken == c.AccessToken {
			return c, fmt.Errorf("distinct Storage owner and Access credentials required")
		}
		if c.ImportVerifierToken != "" && c.ImportVerifierToken == c.AccessToken {
			return c, fmt.Errorf("distinct Storage evidence credential required")
		}
	}
	if len(c.Credentials) == 0 || c.DatabaseURL == "" || c.Bucket == "" || c.OwnerURL == "" || c.OwnerToken == "" {
		return c, fmt.Errorf("database, private S3 bucket, owner adapter and service credentials required")
	}
	seenTokens := map[string]bool{}
	seenPrincipals := map[v.Principal]bool{}
	for _, a := range c.Credentials {
		principal := v.Principal{Subject: a.Subject, Domain: a.Domain}
		if len(a.Token) < 16 || !accessv1.ValidCredential(a.Token) || principal.Validate() != nil || seenTokens[a.Token] || a.Token == c.ImportVerifierToken {
			return c, fmt.Errorf("invalid credential")
		}
		seenTokens[a.Token] = true
		if c.AuthMode == "access" {
			if a.Domain != "service" || len(a.Actions) == 0 || len(a.Actions) > 16 || a.Token == c.OwnerToken || a.Token == c.AccessToken || seenPrincipals[principal] {
				return c, fmt.Errorf("distinct action-scoped Storage service credentials required")
			}
			seenPrincipals[principal] = true
			seenActions := map[string]bool{}
			for _, op := range a.Actions {
				if !internalAction(op) || seenActions[op] {
					return c, fmt.Errorf("invalid Storage service action")
				}
				seenActions[op] = true
			}
		}
	}
	var err error
	c.AuditRetention, err = time.ParseDuration(env("LEAFRUN_STORAGE_AUDIT_RETENTION", "720h"))
	if err != nil || c.AuditRetention < time.Hour || c.AuditRetention > 365*24*time.Hour {
		return c, fmt.Errorf("invalid Storage audit retention")
	}
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
