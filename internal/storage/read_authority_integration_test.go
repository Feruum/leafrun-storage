package storage

import (
	"context"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"github.com/leafrun/leafrun-platform/authn"
)

func privateCatalog(t *testing.T) *DB {
	t.Helper()
	dsn := os.Getenv("STORAGE_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("STORAGE_TEST_DATABASE_URL is not set")
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil || cfg.ConnConfig.Database != "storage_test" {
		t.Fatal("dedicated storage_test required")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "storage_test_" + id()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		admin.Close()
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		p.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		admin.Close()
	})
	d := &DB{Pool: p}
	if e = d.Migrate(ctx); e != nil {
		t.Fatal(e)
	}
	return d
}

type catalogAccess struct{ revoked *atomic.Bool }

func (c catalogAccess) Check(context.Context, accessv1.CheckRequest) (bool, error) {
	return !c.revoked.Load(), nil
}
func (c catalogAccess) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	allowed, err := c.Check(ctx, in)
	if !allowed {
		return accessv1.CheckResponse{}, err
	}
	scope := accessv1.PermissionScope{OrganizationID: "fixture-organization", WorkspaceID: in.Scope.WorkspaceID, ProjectID: in.Scope.ProjectID}
	return accessv1.CheckResponse{Allowed: true, CanonicalScope: &scope}, err
}

type catalogBlob struct {
	Blob
	reads atomic.Int32
}

func (b *catalogBlob) Read(context.Context, Physical) (io.ReadCloser, error) {
	b.reads.Add(1)
	return io.NopCloser(strings.NewReader("pdf")), nil
}

type catalogOwner struct {
	Owner
	revoked       *atomic.Bool
	calls         atomic.Int32
	revokeOnFinal bool
	onFinal       func()
}

func (o *catalogOwner) Publishable(context.Context, v.PublishabilityCheck) (bool, error) {
	if o.calls.Add(1) == 2 {
		if o.revokeOnFinal {
			o.revoked.Store(true)
		}
		if o.onFinal != nil {
			o.onFinal()
		}
	}
	return true, nil
}

func catalogArtifact(t *testing.T, d *DB) string {
	t.Helper()
	ctx := context.Background()
	snapshot, artifactID := id(), id()
	if _, e := d.Pool.Exec(ctx, `INSERT INTO storage_objects(id,workspace,project,class,state,hash,size,physical_key,version,created_at,expires_at) VALUES($1,'workspace','project','snapshot','ready','snapshot-hash',0,'snapshot-key','snapshot-version',clock_timestamp(),NULL),($2,'workspace','project','result','ready','pdf-hash',3,'pdf-key','pdf-version',clock_timestamp(),clock_timestamp()+interval '1 hour')`, snapshot, artifactID); e != nil {
		t.Fatal(e)
	}
	if _, e := d.Pool.Exec(ctx, `INSERT INTO storage_snapshots VALUES($1,'{}','[]')`, snapshot); e != nil {
		t.Fatal(e)
	}
	if _, e := d.Pool.Exec(ctx, `INSERT INTO storage_artifacts VALUES($1,$2,'job','{}')`, artifactID, snapshot); e != nil {
		t.Fatal(e)
	}
	return artifactID
}

func TestPostgresPublicArtifactAuthorityAfterCatalogWait(t *testing.T) {
	for _, metadata := range []bool{false, true} {
		t.Run(map[bool]string{false: "content", true: "metadata"}[metadata], func(t *testing.T) {
			d := privateCatalog(t)
			oid := catalogArtifact(t, d)
			var revoked atomic.Bool
			owner := &catalogOwner{revoked: &revoked}
			blob := &catalogBlob{}
			s := &Service{DB: d, Blob: blob, Owner: owner, Config: Config{AuthMode: "access", Access: catalogAccess{&revoked}, ReadLease: time.Minute}}
			identity := identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session", Version: 1}}
			ctx, e := authn.WithIdentity(context.Background(), identity)
			if e != nil {
				t.Fatal(e)
			}
			lock, e := d.Pool.Begin(ctx)
			if e != nil {
				t.Fatal(e)
			}
			defer lock.Rollback(ctx)
			if _, e = lock.Exec(ctx, "SELECT pg_advisory_xact_lock(710032)"); e != nil {
				t.Fatal(e)
			}
			done := make(chan error, 1)
			go func() {
				if metadata {
					_, err := s.GetArtifact(ctx, identity.Principal, oid)
					done <- err
				} else {
					body, _, _, err := s.OpenRead(ctx, identity.Principal, oid, true)
					if body != nil {
						body.Close()
					}
					done <- err
				}
			}()
			until := time.Now().Add(5 * time.Second)
			for {
				var waiting bool
				if e = d.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%pg_advisory_xact_lock(710032)%')`).Scan(&waiting); e != nil {
					t.Fatal(e)
				}
				if waiting {
					break
				}
				if time.Now().After(until) {
					t.Fatal("public read did not reach catalog wait")
				}
				time.Sleep(10 * time.Millisecond)
			}
			revoked.Store(true)
			if e = lock.Commit(ctx); e != nil {
				t.Fatal(e)
			}
			wantStatus(t, <-done, 404)
			var n int
			if e = d.Pool.QueryRow(ctx, "SELECT count(*) FROM storage_read_leases").Scan(&n); e != nil || n != 0 || blob.reads.Load() != 0 {
				t.Fatalf("unauthorized read escaped: leases=%d reads=%d err=%v", n, blob.reads.Load(), e)
			}
		})
	}
}
