package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"github.com/jackc/pgx/v5"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"io"
	"os"
	"time"
)

type Service struct {
	DB     *DB
	Blob   Blob
	Owner  Owner
	Config Config
}

func (s *Service) authorize(ctx context.Context, p v.Principal, scope v.Scope, op, oid string) error {
	if s.Owner == nil {
		return fail(503, "owner_unavailable")
	}
	allowed, e := s.Owner.Access(ctx, v.AccessCheck{Principal: p, Scope: scope, Operation: op, ObjectID: oid})
	if e != nil {
		return e
	}
	if !allowed {
		return fail(404, "not_found")
	}
	return nil
}
func (s *Service) CreateUpload(ctx context.Context, p v.Principal, in v.CreateUploadRequest) (v.Upload, error) {
	var out v.Upload
	if !validScope(in.Scope) || !validHash(in.SHA256) || in.SizeBytes < 0 || in.SizeBytes > s.Config.MaxUpload || (in.Class != v.ClassResource && in.Class != v.ClassResult && in.Class != v.ClassRecovery) {
		return out, fail(400, "invalid_upload")
	}
	if e := s.authorize(ctx, p, in.Scope, "upload.create", ""); e != nil {
		return out, e
	}
	e := s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		old, e := receipt(ctx, t, p, "upload", in.IdempotencyKey, in)
		if e != nil {
			return e
		}
		if old != "" {
			out, e = upload(ctx, t, old)
			return e
		}
		oid, writer := id(), id()
		var expires *time.Time
		if in.Class == v.ClassRecovery {
			x := now.Add(30 * 24 * time.Hour)
			expires = &x
		}
		_, e = t.Exec(ctx, "INSERT INTO storage_objects(id,workspace,project,class,state,hash,size,physical_key,created_at,expires_at) VALUES($1,$2,$3,$4,'staging',$5,$6,$7,$8,$9)", oid, in.Scope.WorkspaceID, in.Scope.ProjectID, in.Class, in.SHA256, in.SizeBytes, "storage/"+string(in.Class)+"/uploads/"+oid, now, expires)
		if e != nil {
			return e
		}
		_, e = t.Exec(ctx, "INSERT INTO storage_uploads(id,object_id,writer,lease_until,state,creator) VALUES($1,$1,$2,$3,'created',$4)", oid, writer, now.Add(s.Config.UploadLease), principalKey(p))
		if e != nil {
			return e
		}
		if e = saveReceipt(ctx, t, p, "upload", in.IdempotencyKey, in, oid, now); e != nil {
			return e
		}
		out, e = upload(ctx, t, oid)
		return e
	})
	return out, e
}
func upload(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, uid string) (v.Upload, error) {
	o, e := getObject(ctx, q, uid)
	if e != nil {
		return v.Upload{}, e
	}
	u := v.Upload{UploadID: uid, Scope: o.Scope, Class: o.Class, SizeBytes: o.Size, SHA256: o.Hash, CreatedAt: o.Created}
	e = q.QueryRow(ctx, "SELECT state,writer,lease_until,COALESCE(resource_id,''),COALESCE(artifact_id,'') FROM storage_uploads WHERE id=$1", uid).Scan(&u.State, &u.WriterToken, &u.LeaseUntil, &u.ResourceID, &u.ArtifactID)
	return u, e
}
func (s *Service) GetUpload(ctx context.Context, p v.Principal, uid string) (v.Upload, error) {
	u, e := upload(ctx, s.DB.Pool, uid)
	if e != nil {
		return u, e
	}
	if e = s.authorize(ctx, p, u.Scope, "upload.get", uid); e != nil {
		return v.Upload{}, e
	}
	var creator string
	e = s.DB.Pool.QueryRow(ctx, "SELECT creator FROM storage_uploads WHERE id=$1", uid).Scan(&creator)
	if e == nil && creator != principalKey(p) {
		return v.Upload{}, fail(404, "not_found")
	}
	return u, e
}
func writer(u v.Upload, token string, now time.Time) error {
	if token == "" || u.WriterToken != token {
		return fail(403, "invalid_writer")
	}
	if !now.Before(u.LeaseUntil) {
		return fail(409, "writer_expired")
	}
	if u.State == "committed" || u.State == "deleted" {
		return fail(409, "writer_fenced")
	}
	return nil
}
func (s *Service) Renew(ctx context.Context, p v.Principal, uid, token string) (v.Upload, error) {
	out, e := s.GetUpload(ctx, p, uid)
	if e != nil {
		return out, e
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		u, e := upload(ctx, t, uid)
		if e != nil {
			return e
		}
		if e = writer(u, token, now); e != nil {
			return e
		}
		o, e := getObject(ctx, t, uid)
		if e != nil {
			return e
		}
		if o.Expires != nil && !now.Before(*o.Expires) {
			return fail(410, "expired")
		}
		_, e = t.Exec(ctx, "UPDATE storage_uploads SET lease_until=$2 WHERE id=$1", uid, now.Add(s.Config.UploadLease))
		if e != nil {
			return e
		}
		out, e = upload(ctx, t, uid)
		return e
	})
	return out, e
}
func (s *Service) Put(ctx context.Context, p v.Principal, uid, token string, body io.Reader) (v.Upload, error) {
	u, e := s.GetUpload(ctx, p, uid)
	if e != nil {
		return u, e
	}
	if token == "" || u.WriterToken != token {
		return u, fail(403, "invalid_writer")
	}
	f, e := os.CreateTemp(s.Config.Scratch, "leafrun-storage-upload-*")
	if e != nil {
		return u, e
	}
	name := f.Name()
	_, e = s.DB.Pool.Exec(ctx, "INSERT INTO storage_scratch(path,upload_id,state,created_at) VALUES($1,$2,'writing',clock_timestamp())", name, uid)
	if e != nil {
		f.Close()
		os.Remove(name)
		return u, e
	}
	defer s.stopScratch(f, uid)
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(f, h), io.LimitReader(body, u.SizeBytes+1))
	if e != nil {
		return u, e
	}
	if n != u.SizeBytes || hex.EncodeToString(h.Sum(nil)) != u.SHA256 {
		return u, fail(422, "content_mismatch")
	}
	already, canWrite := false, false
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		current, e := upload(ctx, t, uid)
		if e != nil {
			return e
		}
		if current.WriterToken != token {
			return fail(403, "invalid_writer")
		}
		if current.State == "uploaded" || current.State == "committed" {
			already = true
			u = current
			return nil
		}
		if e = writer(current, token, now); e != nil {
			return e
		}
		if current.State == "writing" {
			return nil // A lost response can be recovered by HEAD, never by another PUT.
		}
		canWrite = true
		_, e = t.Exec(ctx, "UPDATE storage_uploads SET state='writing' WHERE id=$1", uid)
		return e
	})
	if e != nil || already {
		return u, e
	}
	key := "storage/" + string(u.Class) + "/uploads/" + uid
	physical, exists, e := s.Blob.Find(ctx, key)
	if e == nil && exists && (physical.Hash != u.SHA256 || physical.Size != u.SizeBytes) {
		e = fail(409, "physical_conflict")
	}
	if e == nil && !exists {
		if !canWrite {
			return u, fail(409, "write_in_progress")
		}
		physical, e = s.Blob.Put(ctx, key, f, u.SHA256, u.SizeBytes)
	}
	if e != nil {
		return u, e
	}
	e = s.DB.tx(context.WithoutCancel(ctx), func(t pgx.Tx, now time.Time) error {
		_, err := t.Exec(context.WithoutCancel(ctx), "INSERT INTO storage_physical_versions(physical_key,version,object_id,created_at) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", physical.Key, physical.Version, uid, now)
		if err != nil {
			return err
		}
		current, err := upload(context.WithoutCancel(ctx), t, uid)
		if err != nil {
			return err
		}
		if current.State == "uploaded" || current.State == "committed" {
			u = current
			return nil
		}
		if err = writer(current, token, now); err != nil {
			return err
		}
		o, err := getObject(context.WithoutCancel(ctx), t, uid)
		if err != nil {
			return err
		}
		if o.State != "staging" {
			return fail(409, "writer_fenced")
		}
		_, err = t.Exec(context.WithoutCancel(ctx), "UPDATE storage_objects SET version=$2 WHERE id=$1", uid, physical.Version)
		if err != nil {
			return err
		}
		_, err = t.Exec(context.WithoutCancel(ctx), "UPDATE storage_uploads SET state='uploaded' WHERE id=$1", uid)
		u, err = upload(context.WithoutCancel(ctx), t, uid)
		return err
	})
	return u, e
}
func resource(o object) v.Resource {
	return v.Resource{ResourceID: o.ID, Scope: o.Scope, Class: o.Class, SHA256: o.Hash, SizeBytes: o.Size, CreatedAt: o.Created}
}
func (s *Service) Commit(ctx context.Context, p v.Principal, uid string, in v.CommitResourceRequest) (v.Resource, error) {
	var out v.Resource
	u, e := s.GetUpload(ctx, p, uid)
	if e != nil {
		return out, e
	}
	e = s.DB.tx(ctx, func(t pgx.Tx, now time.Time) error {
		u, e = upload(ctx, t, uid)
		if e != nil {
			return e
		}
		if u.Class != v.ClassResource && u.Class != v.ClassRecovery {
			return fail(400, "invalid_class")
		}
		old, e := receipt(ctx, t, p, "commit:"+uid, in.IdempotencyKey, in)
		if e != nil {
			return e
		}
		if old != "" {
			o, e := getObject(ctx, t, old)
			out = resource(o)
			return e
		}
		if u.ResourceID != "" {
			return fail(409, "already_committed")
		}
		if e = writer(u, in.WriterToken, now); e != nil {
			return e
		}
		if u.State != "uploaded" {
			return fail(409, "upload_incomplete")
		}
		if u.Class == v.ClassRecovery {
			o, e := getObject(ctx, t, uid)
			if e != nil {
				return e
			}
			if o.Expires != nil && !now.Before(*o.Expires) {
				return fail(410, "expired")
			}
			if _, e = t.Exec(ctx, "UPDATE storage_objects SET state='deleting' WHERE class='recovery' AND state='ready' AND expires_at<=$1", now); e != nil {
				return e
			}
		}
		oid := uid
		var existing string
		e = t.QueryRow(ctx, "SELECT r.id FROM storage_resources r JOIN storage_objects o ON o.id=r.id WHERE workspace=$1 AND project=$2 AND class=$3 AND hash=$4 AND size=$5 AND state='ready' AND (expires_at IS NULL OR expires_at>$6)", u.Scope.WorkspaceID, u.Scope.ProjectID, u.Class, u.SHA256, u.SizeBytes, now).Scan(&existing)
		if e == nil {
			oid = existing
			_, e = t.Exec(ctx, "UPDATE storage_objects SET state='orphan' WHERE id=$1", uid)
		} else if e == pgx.ErrNoRows {
			_, e = t.Exec(ctx, "UPDATE storage_objects SET state='ready' WHERE id=$1", uid)
			if e == nil {
				_, e = t.Exec(ctx, "INSERT INTO storage_resources VALUES($1)", uid)
			}
		}
		if e != nil {
			return e
		}
		_, e = t.Exec(ctx, "UPDATE storage_uploads SET state='committed',resource_id=$2 WHERE id=$1", uid, oid)
		if e != nil {
			return e
		}
		if e = saveReceipt(ctx, t, p, "commit:"+uid, in.IdempotencyKey, in, oid, now); e != nil {
			return e
		}
		o, e := getObject(ctx, t, oid)
		out = resource(o)
		return e
	})
	return out, e
}
