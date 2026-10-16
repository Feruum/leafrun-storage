package storage

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

func internalAction(op string) bool {
	switch op {
	case "upload.create", "upload.get", "upload.put", "upload.renew", "resource.commit", "snapshot.create", "snapshot.get", "resource.read", "artifact.finalize", "pin.acquire", "pin.get", "pin.activate", "pin.release":
		return true
	}
	return false
}
func (s *Service) localServiceAction(p storagev1.Principal, op string) error {
	if p.Domain != "service" || !internalAction(op) {
		return fail(404, "not_found")
	}
	for _, credential := range s.Config.Credentials {
		if credential.Domain == p.Domain && credential.Subject == p.Subject {
			for _, action := range credential.Actions {
				if action == op {
					return nil
				}
			}
		}
	}
	return fail(404, "not_found")
}

func (s *Service) publicIdentity(r *http.Request) (*http.Request, storagev1.Principal, error) {
	cookieName := s.Config.CookieName
	if cookieName == "" {
		cookieName = authn.DefaultSessionCookieName
	}
	selected, transport, err := authn.SelectCredentialsWithCookieNames(r, authn.ConfiguredCookieNames(cookieName, s.Config.GuestCookieName))
	if err != nil {
		return r, storagev1.Principal{}, fail(401, "unauthorized")
	}
	blocked := []string{s.Config.OwnerToken, s.Config.AccessToken, s.Config.ImportVerifierToken}
	for _, credential := range s.Config.Credentials {
		blocked = append(blocked, credential.Token)
	}
	for _, secret := range blocked {
		if secret != "" && (selected.Bearer == secret || selected.SessionCookie == secret) {
			return r, storagev1.Principal{}, fail(401, "unauthorized")
		}
	}
	if s.Config.BrowserGuard.Check(r, transport) != nil {
		return r, storagev1.Principal{}, fail(403, "forbidden")
	}
	if s.Config.Authenticator == nil {
		return r, storagev1.Principal{}, fail(503, "access_unavailable")
	}
	id, err := s.Config.Authenticator.Authenticate(r.Context(), r)
	if err != nil {
		if errors.Is(err, accessv1.ErrUnauthenticated) {
			return r, storagev1.Principal{}, fail(401, "unauthorized")
		}
		return r, storagev1.Principal{}, fail(503, "access_unavailable")
	}
	if id.Validate() != nil || id.Authority.Kind == identityv1.KindDev || id.Authority.Kind == identityv1.KindService || strings.EqualFold(id.Principal.Domain, "service") {
		return r, storagev1.Principal{}, fail(401, "unauthorized")
	}
	ctx, err := authn.WithIdentity(r.Context(), id)
	if err != nil {
		return r, storagev1.Principal{}, fail(503, "access_unavailable")
	}
	return r.WithContext(ctx), id.Principal, nil
}

func (s *Service) currentPublicAccess(ctx context.Context, p storagev1.Principal, scope storagev1.Scope, op, oid string) error {
	id, ok := authn.IdentityFromContext(ctx)
	if !ok || id.Principal != p || s.Config.Access == nil {
		return fail(503, "access_unavailable")
	}
	input := accessv1.FromStorageAccessCheck(storagev1.AccessCheck{Principal: p, Authority: &id.Authority, Scope: scope, Operation: op, ObjectID: oid})
	if canonical, ok := ctx.Value(readAuditScopeKey{}).(accessv1.PermissionScope); ok {
		if canonical.WorkspaceID != scope.WorkspaceID || canonical.ProjectID != scope.ProjectID {
			return fail(404, "not_found")
		}
		input.Scope = canonical
	}
	allowed, err := s.Config.Access.Check(ctx, input)
	if err != nil {
		if errors.Is(err, accessv1.ErrDenied) || errors.Is(err, accessv1.ErrUnauthenticated) {
			return fail(404, "not_found")
		}
		return fail(503, "access_unavailable")
	}
	if !allowed {
		return fail(404, "not_found")
	}
	return nil
}

func (s *Service) fixedIdentity(r *http.Request) (storagev1.Principal, bool) {
	if s.Config.AuthMode != "access" && (s.Config.AuthMode != "dev" || !s.Config.DevFixtures) {
		return storagev1.Principal{}, false
	}
	name := s.Config.CookieName
	if name == "" {
		name = authn.DefaultSessionCookieName
	}
	in, err := authn.CredentialsWithCookieNames(r, authn.ConfiguredCookieNames(name, s.Config.GuestCookieName))
	if err != nil || in.SessionCookie != "" {
		return storagev1.Principal{}, false
	}
	for _, credential := range s.Config.Credentials {
		if len(in.Bearer) == len(credential.Token) && subtle.ConstantTimeCompare([]byte(in.Bearer), []byte(credential.Token)) == 1 {
			return storagev1.Principal{Subject: credential.Subject, Domain: credential.Domain}, true
		}
	}
	return storagev1.Principal{}, false
}
func internalRouteAction(pattern string) string {
	switch pattern {
	case "POST /internal/v1/uploads":
		return "upload.create"
	case "GET /internal/v1/uploads/{id}":
		return "upload.get"
	case "PUT /internal/v1/uploads/{id}/content":
		return "upload.put"
	case "POST /internal/v1/uploads/{id}/renew":
		return "upload.renew"
	case "POST /internal/v1/uploads/{id}/commit":
		return "resource.commit"
	case "POST /internal/v1/uploads/{id}/finalize":
		return "artifact.finalize"
	case "POST /internal/v1/snapshots":
		return "snapshot.create"
	case "GET /internal/v1/snapshots/{id}":
		return "snapshot.get"
	case "GET /internal/v1/resources/{id}", "GET /internal/v1/resources/{id}/content":
		return "resource.read"
	case "POST /internal/v1/pins":
		return "pin.acquire"
	case "GET /internal/v1/pins/{id}":
		return "pin.get"
	case "POST /internal/v1/pins/{id}/activate":
		return "pin.activate"
	case "POST /internal/v1/pins/{id}/release", "DELETE /internal/v1/pins/{id}":
		return "pin.release"
	}
	return ""
}
