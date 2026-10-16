package storage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

type authFixture struct{ err error }

func (a authFixture) Authenticate(context.Context, *http.Request) (identityv1.Identity, error) {
	return identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "human"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}, a.err
}
func TestStoragePublicAndInternalAuthenticationStaySeparate(t *testing.T) {
	guard, _ := authn.NewBrowserGuard([]string{"https://app.example"}, false)
	for _, tc := range []struct {
		name, path, token, cookie string
		err                       error
		status                    int
	}{
		{name: "both-cookie", path: "/v1/artifacts/secret", cookie: "ory_kratos_session=human; leafrun_guest_session=guest", status: 401},
		{name: "service-guest-internal", path: "/internal/v1/pins", token: "service-secret-123456", cookie: "leafrun_guest_session=guest", status: 401},
		{name: "service-on-public", path: "/v1/artifacts/secret", token: "service-secret-123456", status: 401},
		{name: "service-cookie-on-public", path: "/v1/artifacts/secret", cookie: "ory_kratos_session=service-secret-123456", status: 401},
		{name: "user-on-internal", path: "/internal/v1/uploads", token: "human-secret", status: 401},
		{name: "cookie-on-internal", path: "/internal/v1/uploads", cookie: "ory_kratos_session=human-secret", status: 401},
		{name: "wrong-internal-action", path: "/internal/v1/pins", token: "service-secret-123456", status: 404},
		{name: "provider-outage", path: "/v1/artifacts/secret", token: "human-secret", err: accessv1.ErrDependency, status: 503},
		{name: "invalid-user", path: "/v1/artifacts/secret", token: "human-secret", err: accessv1.ErrUnauthenticated, status: 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &Service{Config: Config{AuthMode: "access", Authenticator: authFixture{tc.err}, BrowserGuard: guard, Credentials: []Credential{{Token: "service-secret-123456", Subject: "generation", Domain: "service", Actions: []string{"upload.create"}}}}}
			r := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if strings.HasPrefix(tc.path, "/internal") {
				r.Method = http.MethodPost
			}
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			if tc.cookie != "" {
				r.Header.Set("Cookie", tc.cookie)
			}
			w := httptest.NewRecorder()
			service.Handler().ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d %s", w.Code, tc.status, w.Body)
			}
		})
	}
}

func TestStorageLocalServiceActionsAreClosed(t *testing.T) {
	service := &Service{Config: Config{AuthMode: "access", Credentials: []Credential{{Token: "service-secret-123456", Subject: "generation", Domain: "service", Actions: []string{"snapshot.get"}}}}}
	principal := identityv1.Principal{Subject: "generation", Domain: "service"}
	scope := storagev1.Scope{WorkspaceID: "workspace", ProjectID: "project"}
	if err := service.authorize(context.Background(), principal, scope, "snapshot.get", "snapshot"); err != nil {
		t.Fatal(err)
	}
	if err := service.authorize(context.Background(), principal, scope, "artifact.read", "artifact"); err == nil {
		t.Fatal("internal service acquired public download")
	}
	if err := service.authorize(context.Background(), identityv1.Principal{Subject: "projects", Domain: "service"}, scope, "snapshot.get", "snapshot"); err == nil {
		t.Fatal("unconfigured service accepted")
	}
}
