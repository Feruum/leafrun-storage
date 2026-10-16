package storage

import (
	"context"
	"errors"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	auditv1 "github.com/leafrun/leafrun-contracts/audit/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"github.com/leafrun/leafrun-platform/authn"
	"github.com/leafrun/leafrun-platform/events"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type auditCatalogAccess struct{ catalogAccess }

func (c auditCatalogAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := c.Check(ctx, in)
	scope := accessv1.PermissionScope{OrganizationID: "organization", WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID}
	if !allowed {
		return accessv1.CheckResponse{}, err
	}
	return accessv1.CheckResponse{Allowed: true, CanonicalScope: &scope}, err
}
func TestPostgresDownloadAuthorizationFactSurvivesLeaseCloseAndBlobFailure(t *testing.T) {
	for _, blobFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "blob-failure"}[blobFailure], func(t *testing.T) {
			d := privateCatalog(t)
			oid := catalogArtifact(t, d)
			var revoked atomic.Bool
			blob := &catalogBlob{}
			var selected Blob = blob
			if blobFailure {
				selected = failingAuditBlob{Blob: blob}
			}
			s := &Service{DB: d, Blob: selected, Owner: &catalogOwner{revoked: &revoked}, Config: Config{AuthMode: "access", Access: auditCatalogAccess{catalogAccess{&revoked}}, ReadLease: time.Minute}}
			identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
			ctx, err := authn.WithIdentity(context.Background(), identity)
			if err != nil {
				t.Fatal(err)
			}
			reader, _, _, err := s.OpenRead(ctx, identity.Principal, oid, true)
			if blobFailure {
				if err == nil {
					t.Fatal("blob failure missing")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				reader.Close()
			}
			var raw []byte
			var subject string
			if err = d.Pool.QueryRow(ctx, `SELECT subject,encoded_envelope FROM platform_event_outbox`).Scan(&subject, &raw); err != nil {
				t.Fatal(err)
			}
			envelope, action, err := events.DecodeAuditAction(subject, raw)
			if err != nil {
				t.Fatal(err)
			}
			if action.Action != auditv1.ActionDownloadAuthorized || action.Actor != identity.Principal || action.Authority != identity.Authority || action.Resource.ID != oid || action.Revision != "pdf-version" || envelope.OrganizationID != "organization" {
				t.Fatalf("incorrect fact: %+v", action)
			}
			var count int
			if err = d.Pool.QueryRow(ctx, `SELECT count(*) FROM storage_read_leases`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("lease not closed %d %v", count, err)
			}
			if err = d.Pool.QueryRow(ctx, `SELECT count(*) FROM storage_download_authorizations`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("fact lost %d %v", count, err)
			}
		})
	}
}

type failingAuditBlob struct{ Blob }

func (f failingAuditBlob) Read(context.Context, Physical) (io.ReadCloser, error) {
	return nil, errors.New("private S3 failure")
}

func TestPostgresAuditEnqueueFailureRollsBackDownloadLease(t *testing.T) {
	d := privateCatalog(t)
	oid := catalogArtifact(t, d)
	var revoked atomic.Bool
	blob := &catalogBlob{}
	s := &Service{DB: d, Blob: blob, Owner: &catalogOwner{revoked: &revoked}, Config: Config{AuthMode: "access", Access: auditCatalogAccess{catalogAccess{&revoked}}, ReadLease: time.Minute}}
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	ctx, err := authn.WithIdentity(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = d.Pool.Exec(ctx, `CREATE FUNCTION reject_audit()RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'audit blocked';END$$;CREATE TRIGGER reject_audit BEFORE INSERT ON platform_event_outbox FOR EACH ROW EXECUTE FUNCTION reject_audit()`); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = s.OpenRead(ctx, identity.Principal, oid, true); err == nil {
		t.Fatal("bytes opened without audit")
	}
	var count int
	if err = d.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM storage_read_leases)+(SELECT count(*) FROM storage_download_authorizations)`).Scan(&count); err != nil || count != 0 || blob.reads.Load() != 0 {
		t.Fatalf("download side effects committed: rows%d blobreads%d %v", count, blob.reads.Load(), err)
	}
}

func TestPostgresAuditOutboxLockWaitCannotReuseDownloadAuthorization(t *testing.T) {
	d := privateCatalog(t)
	oid := catalogArtifact(t, d)
	var revoked atomic.Bool
	blob := &catalogBlob{}
	s := &Service{DB: d, Blob: blob, Owner: &catalogOwner{revoked: &revoked}, Config: Config{AuthMode: "access", Access: auditCatalogAccess{catalogAccess{&revoked}}, ReadLease: time.Minute}}
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	ctx, err := authn.WithIdentity(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	lock, err := d.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err = lock.Exec(ctx, `LOCK TABLE platform_event_outbox IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		reader, _, _, e := s.OpenRead(ctx, identity.Principal, oid, true)
		if reader != nil {
			reader.Close()
		}
		done <- e
	}()
	until := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err = d.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE NOT granted AND relation='platform_event_outbox'::regclass)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(until) {
			t.Fatal("read never waited on owned audit outbox")
		}
		time.Sleep(10 * time.Millisecond)
	}
	revoked.Store(true)
	if err = lock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	wantStatus(t, <-done, 404)
	var count int
	if err = d.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM storage_read_leases)+(SELECT count(*) FROM storage_download_authorizations)+(SELECT count(*) FROM platform_event_outbox)`).Scan(&count); err != nil || count != 0 || blob.reads.Load() != 0 {
		t.Fatalf("stale authority escaped after audit wait: %d %d %v", count, blob.reads.Load(), err)
	}
}

func TestPostgresAuditMigrationReadinessAndTamper(t *testing.T) {
	d := privateCatalog(t)
	ctx := context.Background()
	if err := d.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Pool.Exec(ctx, `UPDATE storage_schema_migrations SET checksum='tampered' WHERE version=1`); err != nil {
		t.Fatal(err)
	}
	if d.Ready(ctx) == nil || d.Migrate(ctx) == nil {
		t.Fatal("historical migration mismatch accepted")
	}
}

func TestPostgresAuditRetentionPreservesPendingAuthorization(t *testing.T) {
	d := privateCatalog(t)
	oid := catalogArtifact(t, d)
	var revoked atomic.Bool
	s := &Service{DB: d, Blob: &catalogBlob{}, Owner: &catalogOwner{revoked: &revoked}, Config: Config{AuthMode: "access", Access: auditCatalogAccess{catalogAccess{&revoked}}, ReadLease: time.Minute, AuditRetention: time.Hour}}
	identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
	ctx, err := authn.WithIdentity(context.Background(), identity)
	if err != nil {
		t.Fatal(err)
	}
	body, _, _, err := s.OpenRead(ctx, identity.Principal, oid, true)
	if err != nil {
		t.Fatal(err)
	}
	body.Close()
	if _, err = d.Pool.Exec(ctx, `UPDATE storage_download_authorizations SET authorized_at=clock_timestamp()-interval '2 hours'`); err != nil {
		t.Fatal(err)
	}
	if err = s.pruneReadAudit(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = d.Pool.QueryRow(ctx, `SELECT count(*) FROM storage_download_authorizations`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("pending fact purged %d %v", count, err)
	}
	if _, err = d.Pool.Exec(ctx, `UPDATE platform_event_outbox SET sent_at=clock_timestamp()-interval '2 hours'`); err != nil {
		t.Fatal(err)
	}
	if err = s.pruneReadAudit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = d.Pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM storage_download_authorizations)+(SELECT count(*) FROM platform_event_outbox)`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("completed receipt retained indefinitely %d %v", count, err)
	}
}
