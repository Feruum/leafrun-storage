package storage

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-platform/events"
	"time"
)

type canonicalScopeChecker interface {
	CheckScope(context.Context, accessv1.CheckRequest) (accessv1.CheckResponse, error)
}
type readAuditScopeKey struct{}

func newAuditID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", errors.New("audit identity unavailable")
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func (s *Service) readAuditScope(ctx context.Context, p identityv1.Principal, o object, result bool) (context.Context, *accessv1.PermissionScope, error) {
	// Internal owner reads are fixed service operations, outside the selected
	// public download action. Public Access reads always require their receipt.
	if s.Config.AuthMode == "dev" && s.Config.DevFixtures || p.Domain == "service" {
		return ctx, nil, nil
	}
	if s.Config.AuthMode != "access" {
		return ctx, nil, fail(503, "audit_unavailable")
	}
	identity, ok := authn.IdentityFromContext(ctx)
	if !ok || identity.Principal != p {
		return ctx, nil, fail(503, "audit_unavailable")
	}
	checker, ok := s.Config.Access.(canonicalScopeChecker)
	if !ok {
		return ctx, nil, fail(503, "audit_unavailable")
	}
	operation := "resource.read"
	if result {
		operation = "artifact.read"
	}
	req := accessv1.CheckRequest{Principal: p, Authority: &identity.Authority, Scope: accessv1.PermissionScope{WorkspaceID: o.Scope.WorkspaceID, ProjectID: o.Scope.ProjectID}, Operation: operation, ObjectID: o.ID}
	decision, err := checker.CheckScope(ctx, req)
	if err != nil {
		if errors.Is(err, accessv1.ErrDenied) || errors.Is(err, accessv1.ErrUnauthenticated) {
			return ctx, nil, fail(404, "not_found")
		}
		return ctx, nil, fail(503, "access_unavailable")
	}
	if !decision.Allowed {
		return ctx, nil, fail(404, "not_found")
	}
	scope := decision.CanonicalScope
	if scope == nil || scope.Validate() != nil || scope.OrganizationID == "" || scope.WorkspaceID != o.Scope.WorkspaceID || scope.ProjectID != o.Scope.ProjectID {
		return ctx, nil, fail(503, "audit_unavailable")
	}
	copy := *scope
	return context.WithValue(ctx, readAuditScopeKey{}, copy), &copy, nil
}
func enqueueReadAudit(ctx context.Context, tx pgx.Tx, o object, scope *accessv1.PermissionScope, at time.Time) error {
	if scope == nil {
		return nil
	}
	identity, ok := authn.IdentityFromContext(ctx)
	if !ok {
		return fail(503, "audit_unavailable")
	}
	event, err := newAuditID()
	if err != nil {
		return err
	}
	operation, err := newAuditID()
	if err != nil {
		return err
	}
	// No blob/catalog foreign key: deleting an object or lease cannot erase an
	// unsent immutable authorization fact. Physical keys and errors are excluded.
	if _, err = tx.Exec(ctx, `INSERT INTO storage_download_authorizations(event_id,operation_id,object_id,object_version,organization_id,workspace_id,project_id,actor_subject,actor_domain,authority_kind,authority_id,authority_version,authorized_at)VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, event, operation, o.ID, o.Version, scope.OrganizationID, scope.WorkspaceID, scope.ProjectID, identity.Principal.Subject, identity.Principal.Domain, string(identity.Authority.Kind), identity.Authority.ID, identity.Authority.Version, at); err != nil {
		return err
	}
	kind := auditv1.ResourceStorageObject
	if o.Class == "result" {
		kind = auditv1.ResourceArtifact
	}
	return events.EnqueueAuditAction(ctx, tx, events.AuditActionInput{EventID: event, Scope: *scope, OccurredAt: at, Action: auditv1.Action{SchemaVersion: 1, Producer: auditv1.ProducerStorage, Actor: identity.Principal, Authority: identity.Authority, OperationID: operation, Action: auditv1.ActionDownloadAuthorized, Resource: auditv1.ResourceRef{Kind: kind, ID: o.ID}, Outcome: auditv1.OutcomeSucceeded, Revision: o.Version}})
}
func (d *DB) Ready(ctx context.Context) error {
	n, err := storageLedger(ctx, d.Pool)
	if err != nil {
		return err
	}
	if n != len(storageMigrationFiles) {
		return errors.New("Storage schema upgrade required")
	}
	rows, err := d.Pool.Query(ctx, `SELECT r.event_id,r.operation_id,r.object_id,r.object_version,r.organization_id,r.workspace_id,r.project_id,r.actor_subject,r.actor_domain,r.authority_kind,r.authority_id,r.authority_version,r.authorized_at,o.encoded_envelope FROM storage_download_authorizations r LEFT JOIN platform_event_outbox o ON o.event_id=r.event_id::text LIMIT 0`)
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}
func (s *Service) pruneReadAudit(ctx context.Context) error {
	retention := s.Config.AuditRetention
	if retention == 0 {
		retention = 30 * 24 * time.Hour
	}
	if retention < time.Hour || retention > 365*24*time.Hour {
		return errors.New("invalid audit retention")
	}
	return s.DB.tx(ctx, func(tx pgx.Tx, _ time.Time) error {
		if _, err := tx.Exec(ctx, `DELETE FROM storage_download_authorizations r USING platform_event_outbox o WHERE r.event_id::text=o.event_id AND o.sent_at IS NOT NULL AND o.sent_at<clock_timestamp()-$1*interval '1 microsecond' AND r.authorized_at<clock_timestamp()-$1*interval '1 microsecond'`, retention.Microseconds()); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM platform_event_outbox o WHERE o.sent_at IS NOT NULL AND o.sent_at<clock_timestamp()-$1*interval '1 microsecond' AND NOT EXISTS(SELECT 1 FROM storage_download_authorizations r WHERE r.event_id::text=o.event_id)`, retention.Microseconds())
		return err
	})
}
