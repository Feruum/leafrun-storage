package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"time"
)

func snapshot(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, oid string) (v.Snapshot, error) {
	o, e := getObject(ctx, q, oid)
	if e != nil {
		return v.Snapshot{}, e
	}
	out := v.Snapshot{SnapshotID: oid, Scope: o.Scope, CreatedAt: o.Created}
	var m, r []byte
	e = q.QueryRow(ctx, "SELECT manifest,bindings FROM storage_snapshots WHERE id=$1", oid).Scan(&m, &r)
	if e != nil {
		return out, e
	}
	if e = json.Unmarshal(m, &out.Manifest); e != nil {
		return out, e
	}
	e = json.Unmarshal(r, &out.Resources)
	return out, e
}
func (s *Service) CreateSnapshot(ctx context.Context, p v.Principal, in v.CreateSnapshotRequest) (v.Snapshot, error) {
	var out v.Snapshot
	want, e := bindings(in.Manifest, in.Resources)
	if e != nil {
		return out, e
	}
	if !validScope(in.Scope) {
		return out, fail(400, "invalid_scope")
	}
	if e = s.authorize(ctx, p, in.Scope, "snapshot.create", ""); e != nil {
		return out, e
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		old, e := receipt(ctx, t, p, "snapshot", in.IdempotencyKey, in)
		if e != nil {
			return e
		}
		if old != "" {
			out, e = snapshot(ctx, t, old)
			return e
		}
		for _, r := range in.Resources {
			o, e := getObject(ctx, t, r.ResourceID)
			if e != nil {
				return e
			}
			if o.Scope != in.Scope || o.Class != v.ClassResource {
				return fail(400, "resource_binding_mismatch")
			}
			if e = ready(o, now); e != nil {
				return e
			}
			f := want[fmt.Sprintf("%s:%d:%s", r.Tree, r.FontIndex, r.Path)]
			if o.Hash != f.SHA256 || o.Size != f.Size {
				return fail(400, "resource_binding_mismatch")
			}
			var exists bool
			if e = t.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM storage_resources WHERE id=$1)", o.ID).Scan(&exists); e != nil {
				return e
			}
			if !exists {
				return fail(400, "resource_not_committed")
			}
		}
		oid := id()
		_, e = t.Exec(ctx, "INSERT INTO storage_objects(id,workspace,project,class,state,hash,created_at) VALUES($1,$2,$3,'snapshot','ready',$4,$5)", oid, in.Scope.WorkspaceID, in.Scope.ProjectID, in.Manifest.ID, now)
		if e != nil {
			return e
		}
		_, e = t.Exec(ctx, "INSERT INTO storage_snapshots VALUES($1,$2,$3)", oid, jsonBytes(in.Manifest), jsonBytes(in.Resources))
		if e != nil {
			return e
		}
		for _, r := range in.Resources {
			_, e = t.Exec(ctx, "INSERT INTO storage_snapshot_refs VALUES($1,$2,$3)", oid, r.ResourceID, fmt.Sprintf("%s:%d:%s", r.Tree, r.FontIndex, r.Path))
			if e != nil {
				return e
			}
		}
		if e = saveReceipt(ctx, t, p, "snapshot", in.IdempotencyKey, in, oid, now); e != nil {
			return e
		}
		out, e = snapshot(ctx, t, oid)
		return e
	})
	return out, e
}
func (s *Service) GetSnapshot(ctx context.Context, p v.Principal, oid string) (v.Snapshot, error) {
	out, e := snapshot(ctx, s.DB.Pool, oid)
	if e != nil {
		return out, e
	}
	if e = s.authorize(ctx, p, out.Scope, "snapshot.get", oid); e != nil {
		return v.Snapshot{}, e
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		o, e := getObject(ctx, t, oid)
		if e != nil {
			return e
		}
		return ready(o, now)
	})
	return out, e
}
func artifact(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, oid string) (v.Artifact, error) {
	o, e := getObject(ctx, q, oid)
	if e != nil {
		return v.Artifact{}, e
	}
	out := v.Artifact{ArtifactID: oid, Scope: o.Scope, Hash: o.Hash, SizeBytes: o.Size, CreatedAt: o.Created}
	if o.Expires != nil {
		out.ExpiresAt = *o.Expires
	}
	var b []byte
	e = q.QueryRow(ctx, "SELECT snapshot_id,origin_job,metadata FROM storage_artifacts WHERE id=$1", oid).Scan(&out.SnapshotID, &out.OriginJobID, &b)
	if e != nil {
		return out, e
	}
	e = json.Unmarshal(b, &out.Metadata)
	return out, e
}
func (s *Service) Finalize(ctx context.Context, p v.Principal, uid string, in v.FinalizeArtifactRequest) (v.Artifact, error) {
	var out v.Artifact
	u, e := s.GetUpload(ctx, p, uid)
	if e != nil {
		return out, e
	}
	if e = s.authorize(ctx, p, u.Scope, "artifact.finalize", uid); e != nil {
		return out, e
	}
	if in.OriginJobID == "" || in.SnapshotID == "" || in.Metadata.Filename == "" || in.Metadata.MediaType == "" || in.Metadata.TargetID == "" || in.Metadata.AttemptID == "" {
		return out, fail(400, "invalid_artifact")
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		u, e = upload(ctx, t, uid)
		if e != nil {
			return e
		}
		if u.Class != v.ClassResult {
			return fail(400, "invalid_class")
		}
		old, e := receipt(ctx, t, p, "finalize:"+uid, in.IdempotencyKey, in)
		if e != nil {
			return e
		}
		if old != "" {
			out, e = artifact(ctx, t, old)
			return e
		}
		if u.ArtifactID != "" {
			return fail(409, "already_finalized")
		}
		if e = writer(u, in.WriterToken, now); e != nil {
			return e
		}
		if u.State != "uploaded" {
			return fail(409, "upload_incomplete")
		}
		snap, e := getObject(ctx, t, in.SnapshotID)
		if e != nil {
			return e
		}
		if snap.Scope != u.Scope || snap.Class != v.ClassSnapshot {
			return fail(400, "snapshot_mismatch")
		}
		if e = ready(snap, now); e != nil {
			return e
		}
		expires := now.Add(24 * time.Hour)
		_, e = t.Exec(ctx, "UPDATE storage_objects SET state='ready',created_at=$2,expires_at=$3 WHERE id=$1 AND state='staging'", uid, now, expires)
		if e != nil {
			return e
		}
		_, e = t.Exec(ctx, "INSERT INTO storage_artifacts VALUES($1,$2,$3,$4)", uid, in.SnapshotID, in.OriginJobID, jsonBytes(in.Metadata))
		if e != nil {
			return e
		}
		_, e = t.Exec(ctx, "UPDATE storage_uploads SET state='committed',artifact_id=$1,receipt_fingerprint=$2 WHERE id=$1", uid, fingerprint(in))
		if e != nil {
			return e
		}
		if e = saveReceipt(ctx, t, p, "finalize:"+uid, in.IdempotencyKey, in, uid, now); e != nil {
			return e
		}
		out, e = artifact(ctx, t, uid)
		return e
	})
	return out, e
}
func (s *Service) publication(ctx context.Context, p v.Principal, a v.Artifact) error {
	if e := s.authorize(ctx, p, a.Scope, "artifact.read", a.ArtifactID); e != nil {
		return e
	}
	ok, e := s.Owner.Publishable(ctx, v.PublishabilityCheck{Scope: a.Scope, OriginJobID: a.OriginJobID, ArtifactID: a.ArtifactID})
	if e != nil {
		return e
	}
	if !ok {
		return fail(404, "not_found")
	}
	return nil
}
func (s *Service) GetArtifact(ctx context.Context, p v.Principal, oid string) (v.Artifact, error) {
	a, e := artifact(ctx, s.DB.Pool, oid)
	if e != nil {
		return a, e
	}
	if e = s.publication(ctx, p, a); e != nil {
		return v.Artifact{}, e
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		o, e := getObject(ctx, t, oid)
		if e != nil {
			return e
		}
		return ready(o, now)
	})
	return a, e
}
