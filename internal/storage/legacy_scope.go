package storage

import (
	"context"
	"crypto/subtle"
	"io"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	jsonvalue "github.com/leafrun/leafrun-contracts/jsonvalue/v1"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

// verifySnapshotBinding reads committed catalog evidence without a permission
// callback. It cannot read blobs, mutate pins, or assert organization ownership.
func (s *Service) verifySnapshotBinding(ctx context.Context, in proof.SnapshotBinding) error {
	if in.Validate() != nil {
		return fail(400, "invalid_evidence")
	}
	var valid bool
	err := s.DB.Pool.QueryRow(ctx, `SELECT o.workspace=$2 AND o.project=$3 AND o.class='snapshot' AND o.state='ready' AND o.hash=$4 AND s.manifest->>'id'=$4 AND (o.expires_at IS NULL OR o.expires_at>clock_timestamp()) FROM storage_objects o JOIN storage_snapshots s ON s.id=o.id WHERE o.id=$1`, in.SnapshotID, in.Scope.WorkspaceID, in.Scope.ProjectID, in.ContentID).Scan(&valid)
	if err == pgx.ErrNoRows || err == nil && !valid {
		return fail(404, "not_found")
	}
	if err != nil {
		return fail(503, "evidence_unavailable")
	}
	return nil
}
func (s *Service) handleVerifySnapshotBinding(w http.ResponseWriter, r *http.Request) {
	secret := s.Config.ImportVerifierToken
	cookieName := s.Config.CookieName
	if cookieName == "" {
		cookieName = authn.DefaultSessionCookieName
	}
	selected, e := authn.CredentialsWithCookieNames(r, authn.ConfiguredCookieNames(cookieName, s.Config.GuestCookieName))
	if secret == "" || e != nil || selected.SessionCookie != "" || subtle.ConstantTimeCompare([]byte(selected.Bearer), []byte(secret)) != 1 {
		writeError(w, fail(401, "unauthorized"))
		return
	}
	if authn.RequireMediaType(r, "application/json") != nil {
		writeError(w, fail(400, "invalid_media_type"))
		return
	}
	body, e := io.ReadAll(io.LimitReader(r.Body, proof.Limit+1))
	var in proof.SnapshotBinding
	if e != nil || jsonvalue.Unmarshal(body, &in, proof.Limit) != nil || in.Validate() != nil {
		writeError(w, fail(400, "invalid_evidence"))
		return
	}
	ctx, cancel := context.WithTimeout(authn.WithoutIdentity(r.Context()), 3*time.Second)
	defer cancel()
	if e = s.verifySnapshotBinding(ctx, in); e != nil {
		writeError(w, e)
		return
	}
	respond(w, in, nil)
}
