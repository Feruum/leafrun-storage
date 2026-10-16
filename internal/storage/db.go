package storage

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	v "github.com/leafrun/leafrun-contracts/storage/v1"
	"time"
)

//go:embed migrations/*.sql
var migrations embed.FS

type DB struct{ Pool *pgxpool.Pool }

func OpenDB(ctx context.Context, url string) (*DB, error) {
	p, e := pgxpool.New(ctx, url)
	if e != nil {
		return nil, e
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, e
	}
	return &DB{p}, nil
}
func (d *DB) tx(ctx context.Context, fn func(pgx.Tx, time.Time) error) error {
	tx, e := d.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(710032)"); e != nil {
		return e
	}
	var now time.Time
	if e = tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); e != nil {
		return e
	}
	if e = fn(tx, now); e != nil {
		return e
	}
	return tx.Commit(ctx)
}

type object struct {
	ID                        string
	Scope                     v.Scope
	Class                     v.ObjectClass
	State, Hash, Key, Version string
	Size                      int64
	Created                   time.Time
	Expires                   *time.Time
}

func getObject(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, oid string) (object, error) {
	var o object
	e := q.QueryRow(ctx, "SELECT id,workspace,project,class,state,hash,size,physical_key,version,created_at,expires_at FROM storage_objects WHERE id=$1", oid).Scan(&o.ID, &o.Scope.WorkspaceID, &o.Scope.ProjectID, &o.Class, &o.State, &o.Hash, &o.Size, &o.Key, &o.Version, &o.Created, &o.Expires)
	if errors.Is(e, pgx.ErrNoRows) {
		return o, fail(404, "not_found")
	}
	return o, e
}
func ready(o object, now time.Time) error {
	if o.Expires != nil && !now.Before(*o.Expires) {
		return fail(410, "expired")
	}
	if o.State != "ready" {
		return fail(409, "object_unavailable")
	}
	return nil
}
func principalKey(p v.Principal) string { return p.Domain + ":" + p.Subject }
func receipt(ctx context.Context, t pgx.Tx, p v.Principal, op, key string, input any) (string, error) {
	if key == "" || len(key) > 256 {
		return "", fail(400, "idempotency_key_required")
	}
	var old, oid string
	e := t.QueryRow(ctx, "SELECT fingerprint,object_id FROM storage_idempotency WHERE principal=$1 AND operation=$2 AND key=$3", principalKey(p), op, key).Scan(&old, &oid)
	if errors.Is(e, pgx.ErrNoRows) {
		return "", nil
	}
	if e != nil {
		return "", e
	}
	if old != fingerprint(input) {
		return "", fail(409, "idempotency_conflict")
	}
	return oid, nil
}
func saveReceipt(ctx context.Context, t pgx.Tx, p v.Principal, op, key string, input any, oid string, now time.Time) error {
	_, e := t.Exec(ctx, "INSERT INTO storage_idempotency VALUES($1,$2,$3,$4,$5,$6)", principalKey(p), op, key, fingerprint(input), oid, now)
	return e
}
func jsonBytes(x any) []byte { b, _ := json.Marshal(x); return b }
