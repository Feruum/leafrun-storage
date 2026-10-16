package storage

import (
	"bytes"
	"context"
	"encoding/json"
	proof "github.com/leafrun/leafrun-contracts/ownerproof/v1"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPostgresSnapshotProofUsesCommittedExactOwnerFacts(t *testing.T) {
	d := privateCatalog(t)
	snapshot := id()
	digest := strings.Repeat("a", 64)
	ctx := context.Background()
	if _, e := d.Pool.Exec(ctx, `INSERT INTO storage_objects(id,workspace,project,class,state,hash,created_at) VALUES($1,'old workspace/é','project','snapshot','ready',$2,clock_timestamp())`, snapshot, digest); e != nil {
		t.Fatal(e)
	}
	binding := proof.SnapshotBinding{Scope: v.Scope{WorkspaceID: "old workspace/é", ProjectID: "project"}, SnapshotID: snapshot, ContentID: digest}
	s := &Service{DB: d, Config: Config{ImportVerifierToken: "dedicated-storage-evidence-secret-123"}}
	wantStatus(t, s.verifySnapshotBinding(ctx, binding), 404)
	if _, e := d.Pool.Exec(ctx, `INSERT INTO storage_snapshots VALUES($1,jsonb_build_object('id',$2::text),'[]')`, snapshot, digest); e != nil {
		t.Fatal(e)
	}
	if e := s.verifySnapshotBinding(ctx, binding); e != nil {
		t.Fatal(e)
	}
	for _, change := range []string{"foreign-scope", "foreign-content", "expired", "deleted", "staging"} {
		t.Run(change, func(t *testing.T) {
			in := binding
			if _, e := d.Pool.Exec(ctx, `UPDATE storage_objects SET state='ready',expires_at=NULL WHERE id=$1`, snapshot); e != nil {
				t.Fatal(e)
			}
			switch change {
			case "foreign-scope":
				in.Scope.WorkspaceID = "foreign"
			case "foreign-content":
				in.ContentID = strings.Repeat("b", 64)
			case "expired":
				if _, e := d.Pool.Exec(ctx, `UPDATE storage_objects SET expires_at=clock_timestamp() WHERE id=$1`, snapshot); e != nil {
					t.Fatal(e)
				}
			default:
				if _, e := d.Pool.Exec(ctx, `UPDATE storage_objects SET state=$2 WHERE id=$1`, snapshot, change); e != nil {
					t.Fatal(e)
				}
			}
			wantStatus(t, s.verifySnapshotBinding(ctx, in), 404)
		})
	}
	data, _ := json.Marshal(binding)
	for _, token := range []string{"public-user", "", "dedicated-storage-evidence-secret-123"} {
		r := httptest.NewRequest("POST", "/internal/v1/snapshot-bindings/verify", bytes.NewReader(data))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, r)
		want := 401
		if token == s.Config.ImportVerifierToken {
			want = 404
		}
		if w.Code != want {
			t.Fatalf("credential boundary %d want %d", w.Code, want)
		}
	}
}
